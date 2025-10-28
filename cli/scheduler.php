<?php
declare(strict_types=1);

require_once __DIR__ . '/../inc/db_pg.php';
require_once __DIR__ . '/../inc/optout.php';
require_once __DIR__ . '/../inc/util.php';

const VER = 'v3.33-promo-autolen+auto-done';

// ===== CLI flags =====
$argv  = $GLOBALS['argv'] ?? [];
$DEBUG = in_array('--debug', $argv, true);
$DRY   = in_array('--dry-run', $argv, true);
$FORCE = null;
foreach ($argv as $a) {
  if (preg_match('/^--force-cid=(\d+)/', $a, $m)) { $FORCE = (int)$m[1]; break; }
}

// ===== Logging =====
$LOG = '/var/log/ucrm/scheduler.log';
function slog(array $m): void {
  $m += ['ts' => date('c'), 'ver'=>VER];
  @file_put_contents($GLOBALS['LOG'], json_encode($m, JSON_UNESCAPED_UNICODE)."\n", FILE_APPEND);
  if (!empty($GLOBALS['DEBUG'])) echo json_encode($m, JSON_UNESCAPED_UNICODE)."\n";
}

// ===== Helpers =====
function has_col(PDO $pdo, string $table, string $col): bool {
  $q=$pdo->prepare("SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name=:t AND column_name=:c");
  $q->execute([':t'=>strtolower($table), ':c'=>strtolower($col)]);
  return (bool)$q->fetchColumn();
}
function build_in(array $vals, string $prefix='p'): array {
  $bind=[]; $ph=[]; $i=0;
  foreach ($vals as $v) { $k=':'.$prefix.$i++; $ph[]=$k; $bind[$k]=(string)$v; }
  return [implode(',', $ph), $bind];
}
function clamp(int $v, int $min, int $max): int { return max($min, min($max, $v)); }

/** Lock simples */
function open_lock(): mixed {
  $paths = ['/run/lock/ucrm_scheduler.lock', '/tmp/ucrm_scheduler.lock'];
  foreach ($paths as $p) {
    $h = @fopen($p, 'c');
    if ($h && @flock($h, LOCK_EX|LOCK_NB)) { @chmod($p, 0666); return $h; }
  }
  return null;
}

/** Índice único para idempotência */
function ensure_unique_index(PDO $pdo): bool {
  try {
    $pdo->exec("CREATE UNIQUE INDEX IF NOT EXISTS uidx_outbox_campaign_user ON crm_outbox(campaign_id, username)");
    return true;
  } catch (Throwable $e) {
    slog(['lvl'=>'WARN','where'=>'ensure_unique_index','err'=>$e->getMessage()]);
    return false;
  }
}

/** MSISDN de indicativo+telefone (só dígitos) */
function to_msisdn(?string $cc, ?string $phone): string {
  $cc = preg_replace('/\D+/', '', (string)$cc);
  $ph = preg_replace('/\D+/', '', (string)$phone);
  if ($ph === '') return '';
  if ($cc !== '') {
    if (str_starts_with($ph, $cc)) return $ph;
    return $cc.$ph;
  }
  if (strlen($ph)===9 && ($ph[0]==='9'||$ph[0]==='2')) return '351'.$ph;
  return $ph;
}

/** Endereços (email/sms) do snapshot */
function fetch_segment_contacts(PDO $pdo, int $segmentId, array $users): array {
  if (!$users) return [];
  [$in,$bind] = build_in($users,'u');
  $st = $pdo->prepare("
    SELECT DISTINCT ON (username)
           username,
           COALESCE(email_pessoal,'')        AS email,
           COALESCE(indicativo_telefone1,'') AS cc,
           COALESCE(telefone_residencia,'')  AS phone
      FROM crm_segment_members
     WHERE segment_id=:sid
       AND username IN ($in)
  ORDER BY username, snapshot_at DESC
  ");
  $st->execute([':sid'=>$segmentId]+$bind);
  $out=[];
  while ($r=$st->fetch(PDO::FETCH_ASSOC)) {
    $out[(string)$r['username']] = [
      'email'=>(string)$r['email'],
      'sms'  =>to_msisdn($r['cc'], $r['phone']),
    ];
  }
  return $out;
}

/** Alfabeto usado na geração (A..Z sem I/O + dígitos 2..9) → 32 símbolos */
function promo_alphabet_size(): int { return 32; }

/**
 * Tamanho mínimo do código aleatório (sem prefix), para população N, alvo p<=1e-6.
 * Aproximação do problema do aniversário:  P(colisão) ≈ 1 - exp(-n(n-1)/(2*Ω))
 * Exigimos Ω >= n(n-1)/(2 ln(1/(1-p)))  ~ n(n-1)/(2p) para p pequeno.
 */
function recommend_len_for_population(int $n, int $alphabet=32, float $p=1e-6): int {
  if ($n <= 1) return 4;
  $requiredSpace = (int)ceil(($n * ($n - 1)) / (2.0 * max($p, 1e-12)));
  $len = 1; $space = $alphabet;
  while ($space < $requiredSpace && $len < 40) { $space *= $alphabet; $len++; }
  return clamp($len, 4, 40);
}

/** Gera códigos */
function gen_code(int $len=10, string $prefix=''): string {
  $alph='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  $out=$prefix;
  for($i=0;$i<$len;$i++) $out.=$alph[random_int(0,strlen($alph)-1)];
  return $out;
}

/**
 * Promo por DB (sem API):
 *  - Se existir code (promotion_code.assigned_to=username):
 *      - Se tiver redeem (promotion_usage por promotion_id+username) → skip (redeemed).
 *      - Senão usa o existente (promo_have++).
 *  - Senão cria um novo (promo_new++).
 *  - Erro ao criar → promo_fail e NÃO enfileira.
 * - O comprimento efetivo do código é max(requested, recommend_len_for_population(segment_size)).
 *
 * Retorna: ['ok'=>bool, 'why'=>?string, 'code'=>?string, 'new'=>bool]
 */
function ensure_promo_for_user(
  PDO $pdo,
  int $promotionId,
  string $username,
  array $gen,
  int $cid,
  array &$metrics,
  int $segmentSize
): array {
  if ($promotionId<=0) return ['ok'=>false,'why'=>'no_pid','code'=>null,'new'=>false];

  // promoção ativa e válida
  $p=$pdo->prepare("SELECT 1 FROM public.promotion
                     WHERE id=:pid AND type='BULK_USER' AND status='active'
                       AND starts_at<=NOW() AND ends_at>=NOW()");
  $p->execute([':pid'=>$promotionId]);
  if (!$p->fetchColumn()) return ['ok'=>false,'why'=>'promo_inactive','code'=>null,'new'=>false];

  $reqLen = (int)($gen['length'] ?? 0);
  $autoLen = recommend_len_for_population($segmentSize, promo_alphabet_size(), 1e-6);
  $len  = clamp(max($reqLen, $autoLen, 4), 4, 40);

  if ($reqLen > 0 && $len > $reqLen) {
    slog(['lvl'=>'INFO','cid'=>$cid,'u'=>$username,'what'=>'PROMO_LEN_BUMP','requested'=>$reqLen,'effective'=>$len,'pop'=>$segmentSize]);
  }

  $pref = (string)($gen['prefix'] ?? '');
  $vf   = isset($gen['valid_from']) && $gen['valid_from']!=='' ? (string)$gen['valid_from'] : null;
  $vu   = isset($gen['valid_until'])&& $gen['valid_until']!==''? (string)$gen['valid_until']: null;
  $note = (string)($gen['note'] ?? 'ucrm');

  $hasVF   = has_col($pdo,'promotion_code','valid_from');
  $hasVU   = has_col($pdo,'promotion_code','valid_until');
  $hasNote = has_col($pdo,'promotion_code','note');

  $pdo->beginTransaction();
  try {
    // lock registo existente
    $lock=$pdo->prepare("SELECT code FROM public.promotion_code
                          WHERE promotion_id=:pid AND assigned_to=:u
                          ORDER BY id DESC
                          LIMIT 1
                          FOR UPDATE");
    $lock->execute([':pid'=>$promotionId, ':u'=>$username]);
    $code=(string)($lock->fetchColumn() ?: '');

    if ($code!=='') {
      $rd=$pdo->prepare("SELECT 1 FROM public.promotion_usage WHERE promotion_id=:pid AND username=:u LIMIT 1");
      $rd->execute([':pid'=>$promotionId, ':u'=>$username]);
      if ($rd->fetchColumn()) {
        $pdo->commit();
        $metrics['skip_redeemed']=($metrics['skip_redeemed']??0)+1;
        return ['ok'=>false,'why'=>'redeemed','code'=>null,'new'=>false];
      }
      $pdo->commit();
      $metrics['promo_have']=($metrics['promo_have']??0)+1;
      return ['ok'=>true,'why'=>null,'code'=>$code,'new'=>false];
    }

    // criar novo
    $cols=['code','promotion_id','assigned_to']; $vals=[':code',':pid',':u'];
    $bind=[':pid'=>$promotionId, ':u'=>$username];
    if ($hasVF)   { $cols[]='valid_from';  $vals[]=':vf';   $bind[':vf']=$vf; }
    if ($hasVU)   { $cols[]='valid_until'; $vals[]=':vu';   $bind[':vu']=$vu; }
    if ($hasNote) { $cols[]='note';        $vals[]=':note'; $bind[':note']=$note; }
    $sqlIns="INSERT INTO public.promotion_code (".implode(',',$cols).") VALUES (".implode(',',$vals).")";

    $tries=0; $ok=false; $new='';
    while ($tries++<12) { // dá mais folga quando a população é grande
      $new=gen_code($len,$pref);
      try {
        $pdo->prepare($sqlIns)->execute([':code'=>$new]+$bind);
        $ok=true; break;
      } catch (PDOException $e) {
        $msg=$e->getMessage();
        if (stripos($msg,'duplicate')!==false || stripos($msg,'unique')!==false || stripos($msg,'violates unique')!==false) {
          // alguém pode ter criado entretanto
          $re=$pdo->prepare("SELECT code FROM public.promotion_code WHERE promotion_id=:pid AND assigned_to=:u LIMIT 1");
          $re->execute([':pid'=>$promotionId, ':u'=>$username]);
          $already=(string)($re->fetchColumn() ?: '');
          if ($already!=='') { $new=$already; $ok=true; break; }
          continue; // colisão do code, tenta novo
        }
        throw $e;
      }
    }
    if (!$ok) { $pdo->rollBack(); $metrics['promo_fail']=($metrics['promo_fail']??0)+1; return ['ok'=>false,'why'=>'create_failed','code'=>null,'new'=>false]; }

    $pdo->commit();
    $metrics['promo_new']=($metrics['promo_new']??0)+1;
    return ['ok'=>true,'why'=>null,'code'=>$new,'new'=>true];

  } catch (Throwable $e) {
    if ($pdo->inTransaction()) $pdo->rollBack();
    $metrics['promo_fail']=($metrics['promo_fail']??0)+1;
    slog(['lvl'=>'EXC','where'=>'ensure_promo_for_user','pid'=>$promotionId,'u'=>$username,'cid'=>$cid,'err'=>$e->getMessage()]);
    return ['ok'=>false,'why'=>'create_failed','code'=>null,'new'=>false];
  }
}

// ===== Execução =====
$lock=open_lock();
if(!$lock){ slog(['lvl'=>'SKIP','why'=>'locked']); exit; }

$pdo = pg_pdo();
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

slog(['lvl'=>'START','debug'=>$DEBUG,'dry'=>$DRY,'ver'=>VER]);

$UNIQUE_OK = ensure_unique_index($pdo);

// Que campanhas processar
$campaigns=[];
if ($FORCE!==null) {
  $s=$pdo->prepare("SELECT * FROM crm_campaigns WHERE id=:id LIMIT 1");
  $s->execute([':id'=>$FORCE]);
  if ($r=$s->fetch(PDO::FETCH_ASSOC)) $campaigns[]=$r;
} else {
  $campaigns=$pdo->query("SELECT * FROM crm_campaigns WHERE status IN ('running','scheduled') ORDER BY id ASC")->fetchAll(PDO::FETCH_ASSOC);
}

$hasAddr  = has_col($pdo,'crm_outbox','address');
$hasSched = has_col($pdo,'crm_outbox','schedule_at');

foreach($campaigns as $c){
  $cid=(int)$c['id'];
  $ch =(string)$c['channel'];
  $seg=(int)$c['segment_id'];
  $cap=max(0,(int)($c['frequency_cap'] ?? 0));
  $status=(string)$c['status'];
  $start=$c['start_at'] ?? ($c['schedule_at'] ?? null);
  $end  =$c['end_at']   ?? null;

  $nowTs=time();
  $stTs=$start?strtotime((string)$start):null;
  $enTs=$end?strtotime((string)$end):null;

  if ($status==='scheduled') {
    if ($stTs!==null && $stTs>$nowTs) { slog(['lvl'=>'WAIT','cid'=>$cid,'start_at'=>$start]); continue; }
    $pdo->prepare("UPDATE crm_campaigns SET status='running' WHERE id=:id AND status='scheduled'")->execute([':id'=>$cid]);
    $status='running';
  }
  if ($stTs!==null && $nowTs<$stTs) { slog(['lvl'=>'WAIT','cid'=>$cid,'start_at'=>$start]); continue; }
  if ($seg<=0) { slog(['lvl'=>'SKIP','cid'=>$cid,'why'=>'no_segment']); continue; }

  // Template
  $tplId=(int)($c['template_id'] ?? 0);
  if ($tplId<=0) { slog(['lvl'=>'SKIP','cid'=>$cid,'why'=>'no_template']); continue; }
  $t=$pdo->prepare("SELECT * FROM crm_templates WHERE id=:id");
  $t->execute([':id'=>$tplId]);
  $tpl=$t->fetch(PDO::FETCH_ASSOC);
  if(!$tpl){ slog(['lvl'=>'SKIP','cid'=>$cid,'why'=>'template_missing','tpl'=>$tplId]); continue; }

  $blob = ($tpl['subject'] ?? '').($tpl['body_html'] ?? '').($tpl['body_text'] ?? '');
  $needsPromo = (strpos($blob,'{{promo_code}}') !== false);

  // UTM com settings de promo (opcionais)
  $utm = $c['utm_tags'];
  if (is_string($utm)) $utm = json_decode($utm,true) ?: [];
  if (!is_array($utm)) $utm = [];
  $pid = (int)($utm['promotion_id'] ?? 0);
  $pg  = (array)($utm['promo_gen'] ?? []);
  $assignPromo = !empty($utm['assign_promo']); // se false e template pede promo, envia sem promo (promo_none)

  // Universo do segmento
  $memb=$pdo->prepare("SELECT DISTINCT username FROM crm_segment_members WHERE segment_id=:sid");
  $memb->execute([':sid'=>$seg]);
  $usernames=array_map('strval',$memb->fetchAll(PDO::FETCH_COLUMN));
  $segmentSize = count($usernames);

  if ($segmentSize === 0) {
    $outboxTotal=(int)$pdo->query("SELECT COUNT(*) FROM crm_outbox WHERE campaign_id={$cid}")->fetchColumn();
    $pending   =(int)$pdo->query("SELECT COUNT(*) FROM crm_outbox WHERE campaign_id={$cid} AND status='queued'")->fetchColumn();
    if ($outboxTotal>0 && $pending===0) {
      if (!$DRY) $pdo->prepare("UPDATE crm_campaigns SET status='done' WHERE id=:id")->execute([':id'=>$cid]);
      slog(['lvl'=>'AUTO_DONE','cid'=>$cid,'outbox_total'=>$outboxTotal,'pending'=>$pending,'enqueued_now'=>0]);
    } else {
      slog(['lvl'=>'OK','cid'=>$cid,'users_total'=>0,'enqueued'=>0]);
    }
    continue;
  }

  $batchSize=500; $insCount=0; $skOpt=0; $skCap=0; $skAddr=0; $skRedeem=0; $skDup=0;
  $m=['addr_found'=>0,'addr_missing'=>0,'promo_new'=>0,'promo_have'=>0,'promo_fail'=>0,'skip_redeemed'=>0,'promo_none'=>0];

  for ($i=0; $i<$segmentSize; $i+=$batchSize) {
    $batch = array_slice($usernames, $i, $batchSize);
    if (!$batch) break;

    [$in,$bind] = build_in($batch,'u');

    // opt-out
    $oo=$pdo->prepare("SELECT username FROM crm_optout WHERE channel=:ch AND username IN ($in)");
    $oo->execute([':ch'=>$ch]+$bind);
    $opt=array_flip(array_map('strval',$oo->fetchAll(PDO::FETCH_COLUMN)));

    // cap 7d
    $fc=[];
    if ($cap>0) {
      $q=$pdo->prepare("SELECT username, COUNT(*) AS cnt
                          FROM crm_outbox
                         WHERE campaign_id=:cid
                           AND username IN ($in)
                           AND created_at >= NOW() - INTERVAL '7 days'
                      GROUP BY username");
      $q->execute([':cid'=>$cid]+$bind);
      foreach ($q->fetchAll(PDO::FETCH_ASSOC) as $r) $fc[(string)$r['username']] = (int)$r['cnt'];
    }

    // contactos
    $contacts = fetch_segment_contacts($pdo, $seg, $batch);

    // CHECK push tokens
    $pushChk=$pdo->prepare("SELECT 1 FROM crm_devices WHERE username=:u AND COALESCE(fcm_token,'')<>'' LIMIT 1");

    // INSERT base
    $cols=['username','channel','payload','status','campaign_id'];
    $vals=[':u',':ch',':p',"'queued'",':cid'];
    if ($hasSched){ $cols[]='schedule_at'; $vals[]='NOW()'; }
    if ($hasAddr) { $cols[]='address';     $vals[]=':addr'; }
    $sqlIns="INSERT INTO crm_outbox (".implode(',',$cols).") VALUES (".implode(',',$vals).")";
    if ($UNIQUE_OK) $sqlIns.=" ON CONFLICT (campaign_id, username) DO NOTHING RETURNING id";
    $ins=$pdo->prepare($sqlIns);

    $exists=null;
    if(!$UNIQUE_OK){
      $exists=$pdo->prepare("SELECT 1 FROM crm_outbox WHERE campaign_id=:cid AND username=:u LIMIT 1");
    }

    foreach ($batch as $u) {
      if (isset($opt[$u])) { $skOpt++; continue; }
      if ($cap>0 && (($fc[$u] ?? 0) >= $cap)) { $skCap++; continue; }

      // endereço
      $addr=''; $okAddr=false;
      if ($ch==='email') {
        $addr=(string)($contacts[$u]['email'] ?? '');
        $okAddr=($addr!=='');
      } elseif ($ch==='sms') {
        $addr=(string)($contacts[$u]['sms'] ?? '');
        $okAddr=($addr!=='');
      } elseif ($ch==='push') {
        $pushChk->execute([':u'=>$u]); $okAddr=(bool)$pushChk->fetchColumn();
      } else {
        continue;
      }
      if ($okAddr) $m['addr_found']++; else { $m['addr_missing']++; $skAddr++; continue; }

      // Promo (apenas se o template pede)
      $promoCode=null;
      if ($needsPromo) {
        if ($pid>0 && $assignPromo) {
          $pr=ensure_promo_for_user($pdo,$pid,$u,$pg,$cid,$m,$segmentSize);
          if (!$pr['ok']) {
            if ($pr['why']==='redeemed'){ $skRedeem++; continue; }
            $m['promo_fail']++; continue; // erro criação/obtenção
          }
          $promoCode = $pr['code'];
        } else {
          // Sem promoção configurada → segue com promo_code vazio
          $m['promo_none'] = ($m['promo_none'] ?? 0) + 1;
          $promoCode = null;
          if ($DEBUG) slog(['lvl'=>'INFO','cid'=>$cid,'u'=>$u,'why'=>'no_promo_config_send_blank']);
        }
      }

      // Payload (worker renderiza placeholders + unsubscribe/ref)
      $payload=[
        'to'        => ($ch==='email' ? $addr : null),
        'subject'   => (string)($tpl['subject'] ?? ''),
        'body_html' => (string)($tpl['body_html'] ?? ''),
        'body_text' => (string)($tpl['body_text'] ?? ''),
        'title'     => (string)($tpl['subject'] ?? ''),
        'body'      => (string)($tpl['body_text'] ?? ($tpl['body_html'] ?? '')),
        'promo_code'=> $promoCode, // pode vir null → worker substitui por ""
      ];

      if ($DRY) {
        $insCount++;
        if ($DEBUG) echo json_encode(['dry'=>true,'cid'=>$cid,'u'=>$u,'ch'=>$ch,'addr'=>$addr,'promo'=>$promoCode], JSON_UNESCAPED_UNICODE)."\n";
        continue;
      }

      if (!$UNIQUE_OK) {
        $exists->execute([':cid'=>$cid, ':u'=>$u]);
        if ($exists->fetchColumn()) { $skDup++; continue; }
      }

      $ins->bindValue(':u',$u);
      $ins->bindValue(':ch',$ch);
      $ins->bindValue(':p',json_encode($payload, JSON_UNESCAPED_UNICODE));
      $ins->bindValue(':cid',$cid, PDO::PARAM_INT);
      if ($hasAddr) $ins->bindValue(':addr',$addr);
      $ins->execute();

      if ($UNIQUE_OK) {
        $newId=$ins->fetchColumn();
        if ($newId) $insCount++; else $skDup++;
      } else {
        $insCount++;
      }
    } // foreach user
  } // batches

  // AUTO_DONE consolidado
  $outboxTotal = (int)$pdo->query("SELECT COUNT(*) FROM crm_outbox WHERE campaign_id={$cid}")->fetchColumn();
  $pending     = (int)$pdo->query("SELECT COUNT(*) FROM crm_outbox WHERE campaign_id={$cid} AND status='queued'")->fetchColumn();

  if (($pending===0 && $insCount===0) || ($enTs!==null && time()>$enTs && $pending===0)) {
    if (!$DRY) $pdo->prepare("UPDATE crm_campaigns SET status='done' WHERE id=:id")->execute([':id'=>$cid]);
    slog(['lvl'=>'AUTO_DONE','cid'=>$cid,'outbox_total'=>$outboxTotal,'pending'=>$pending,'enqueued_now'=>$insCount]);
  }

  slog([
    'lvl'=>'OK','cid'=>$cid,
    'users_total'=>$segmentSize,
    'enqueued'=>$insCount,'skip_optout'=>$skOpt,'skip_cap7d'=>$skCap,'skip_noaddr'=>$skAddr,
    'skip_redeemed'=>$skRedeem,'skip_dup'=>$skDup,
    'addr_found'=>$m['addr_found'],'addr_missing'=>$m['addr_missing'],
    'promo_new'=>$m['promo_new'],'promo_have'=>$m['promo_have'],'promo_fail'=>$m['promo_fail'],
    'promo_none'=>$m['promo_none'] ?? 0
  ]);
} // foreach campaign

slog(['lvl'=>'DONE','ver'=>VER]);

