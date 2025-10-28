<?php
declare(strict_types=1);
require_once __DIR__ . '/../inc/db_pg.php';
require_once __DIR__ . '/../inc/db_oracle.php';
require_once __DIR__ . '/../inc/optout.php';

$LOG = '/var/log/ucrm/scheduler.log';
function logl(array $m){ file_put_contents($GLOBALS['LOG'], json_encode($m, JSON_UNESCAPED_UNICODE)."\n", FILE_APPEND); }
function has_col(PDO $pdo, string $t, string $c): bool {
  $q = $pdo->prepare("SELECT 1 FROM information_schema.columns WHERE table_name=:t AND column_name=:c");
  $q->execute([':t'=>strtolower($t), ':c'=>strtolower($c)]);
  return (bool)$q->fetchColumn();
}

$pdo = pg_pdo();
logl(['ts'=>date('c'),'lvl'=>'START','v'=>'2.1-address']);

$campQ = $pdo->query("SELECT * FROM crm_campaigns WHERE status='running' ORDER BY id DESC");
$campaigns = $campQ->fetchAll(PDO::FETCH_ASSOC);

foreach ($campaigns as $c) {
  $cid = (int)$c['id'];
  $ch  = (string)($c['channel'] ?? 'email');
  $seg = (int)($c['segment_id'] ?? 0);
  $cap = max(0, (int)($c['frequency_cap'] ?? 0)); // 0 = sem cap

  // template (genérico ou por canal)
  $tplId = null;
  if (!empty($c['template_id'])) $tplId = (int)$c['template_id']; else {
    $k = $ch.'_template_id'; if (!empty($c[$k])) $tplId = (int)$c[$k];
  }
  if (!$tplId) { logl(['ts'=>date('c'),'lvl'=>'SKIP','cid'=>$cid,'why'=>'no_template','channel'=>$ch]); continue; }
  if ($seg<=0) { logl(['ts'=>date('c'),'lvl'=>'SKIP','cid'=>$cid,'why'=>'no_segment']); continue; }

  // carrega template
  $tt = $pdo->prepare("SELECT * FROM crm_templates WHERE id=:id");
  $tt->execute([':id'=>$tplId]);
  $tpl = $tt->fetch(PDO::FETCH_ASSOC);
  if (!$tpl) { logl(['ts'=>date('c'),'lvl'=>'SKIP','cid'=>$cid,'why'=>'template_missing','tpl'=>$tplId]); continue; }

  // membros do segmento
  $mem = $pdo->prepare("SELECT username FROM crm_segment_members WHERE segment_id=:sid ORDER BY username");
  $mem->execute([':sid'=>$seg]);
  $usernames = array_map('strval', $mem->fetchAll(PDO::FETCH_COLUMN));
  if (!$usernames) { logl(['ts'=>date('c'),'lvl'=>'OK','cid'=>$cid,'users'=>0]); continue; }

  // placeholders presentes?
  $needsName  = str_contains(($tpl['subject']??'').($tpl['body_html']??'').($tpl['body_text']??''), '{{nome}}');
  $needsPromo = str_contains(($tpl['subject']??'').($tpl['body_html']??'').($tpl['body_text']??''), '{{promo_code}}');
  $needEmail  = ($ch === 'email'); // vamos gravar address

  $batchSize = 500; $insCount=0; $skOpt=0; $skCap=0;
  $hasSched = has_col($pdo,'crm_outbox','schedule_at');
  $hasAddr  = has_col($pdo,'crm_outbox','address');

  for ($i=0; $i<count($usernames); $i+=$batchSize) {
    $batch = array_slice($usernames, $i, $batchSize);
    if (!$batch) break;

    // OPT-OUT set
    $ph=[]; $par=[];
    foreach ($batch as $k=>$u){ $ph[]=":u$k"; $par[":u$k"]=$u; }
    $par[':ch'] = $ch;
    $oo = $pdo->prepare("SELECT username FROM crm_optout WHERE channel=:ch AND username IN (".implode(',',$ph).")");
    $oo->execute($par);
    $optset = array_flip(array_map('strval',$oo->fetchAll(PDO::FETCH_COLUMN)));

    // FREQ CAP (7 dias)
    $fc = [];
    if ($cap>0) {
      $q = $pdo->prepare("SELECT username, COUNT(*) AS cnt
                            FROM crm_outbox
                           WHERE campaign_id=:cid
                             AND username IN (".implode(',',$ph).")
                             AND created_at >= NOW() - INTERVAL '7 days'
                        GROUP BY username");
      $p2 = [':cid'=>$cid] + $par;
      $q->execute($p2);
      foreach ($q->fetchAll(PDO::FETCH_ASSOC) as $r) $fc[(string)$r['username']] = (int)$r['cnt'];
    }

    // Oracle: nome e email (apenas se necessário)
    $nameMap  = [];
    $emailMap = [];
    if ($needsName || $needEmail) {
      $bind=[]; $in=[]; $idx=0;
      foreach ($batch as $u){ $k=':C'.($idx++); $in[]=$k; $bind[$k]=$u; }
      try {
        $rows = oci_all("SELECT TO_CHAR(CODIGOCLIENTE) AS U, NOME, EMAILPESSOAL
                           FROM TRADER.AGC_CLIENTES
                          WHERE TO_CHAR(CODIGOCLIENTE) IN (".implode(',',$in).")", $bind);
        foreach ($rows as $r) {
          $U = (string)$r['U'];
          if ($needsName)  $nameMap[$U]  = (string)($r['NOME'] ?? '');
          if ($needEmail)  $emailMap[$U] = (string)($r['EMAILPESSOAL'] ?? '');
        }
      } catch (Throwable $e) { /* ignora */ }
    }

    // PROMO code (se necessário)
    $promoMap = [];
    if ($needsPromo) {
      $qp = $pdo->prepare("SELECT DISTINCT ON (username) username, promo_code
                             FROM crm_promotions_assigned
                            WHERE username IN (".implode(',',$ph).")
                         ORDER BY username, assigned_at DESC");
      $qp->execute($par);
      foreach ($qp->fetchAll(PDO::FETCH_ASSOC) as $r) $promoMap[(string)$r['username']] = (string)$r['promo_code'];
    }

    // INSERT preparado
    $cols = ['username','channel','payload','status','campaign_id'];
    $vals = [':u',':ch',':p',"'queued'",':cid'];
    if ($hasSched) { $cols[]='schedule_at'; $vals[]='NOW()'; }
    if ($hasAddr)  { $cols[]='address';     $vals[]=':addr'; }
    $sql = "INSERT INTO crm_outbox (".implode(',',$cols).") VALUES (".implode(',',$vals).")";
    $ins = $pdo->prepare($sql);

    foreach ($batch as $u) {
      if (isset($optset[$u])) { $skOpt++; continue; }
      if ($cap>0 && (($fc[$u] ?? 0) >= $cap)) { $skCap++; continue; }

      $nome  = $nameMap[$u]  ?? '';
      $promo = $promoMap[$u] ?? '';

      $repl = fn(string $s)=> str_replace(['{{username}}','{{nome}}','{{promo_code}}'], [$u,$nome,$promo], $s);
      $subject   = $repl((string)($tpl['subject']   ?? ''));
      $body_html = $repl((string)($tpl['body_html'] ?? ''));
      $body_text = $repl((string)($tpl['body_text'] ?? ''));

      if ($ch==='email') {
        $unsub = build_unsub_url($u,'email');
        foreach (['subject','body_html','body_text'] as $k) {
          $$k = str_replace('{{unsubscribe_url}}', $unsub, $$k);
        }
      }

      $payload = [
        'subject'=>$subject, 'body_html'=>$body_html, 'body_text'=>$body_text,
        'title'=>$subject, 'body'=>($body_html!==''?$body_html:$body_text),
      ];
      $ins->bindValue(':u',$u);
      $ins->bindValue(':ch',$ch);
      $ins->bindValue(':p', json_encode($payload, JSON_UNESCAPED_UNICODE));
      $ins->bindValue(':cid',$cid, PDO::PARAM_INT);
      if ($hasAddr) {
        $addr = ($ch==='email') ? ($emailMap[$u] ?? '') : null;
        $ins->bindValue(':addr', $addr);
      }
      $ins->execute();
      $insCount++;
    }
  }

  logl(['ts'=>date('c'),'lvl'=>'OK','cid'=>$cid,'users_total'=>count($usernames),'enqueued'=>$insCount,'skip_optout'=>$skOpt,'skip_cap7d'=>$skCap]);
}

logl(['ts'=>date('c'),'lvl'=>'DONE']);
