<?php
declare(strict_types=1);

require_once __DIR__.'/../inc/db_pg.php';
require_once __DIR__.'/../inc/util.php';
require_once __DIR__.'/../inc/optout.php';
require_once __DIR__.'/../inc/payload_normalizer.php';
require_once __DIR__.'/../inc/sendmail.php';      // rt_sendmail + whitelist
require_once __DIR__.'/../inc/sms.php';           // send_sms (usa SMS Express) — sem sms_log aqui
require_once __DIR__.'/../inc/fcm.php';
require_once __DIR__.'/../inc/db_oracle.php';
require_once __DIR__.'/../inc/config.local.php';  // SMTP/SMS/FCM/env
require_once __DIR__.'/../inc/referral.php';      // ref_build_url()

// CLI flags (opcional)
$argv   = $GLOBALS['argv'] ?? [];
$DEBUG  = in_array('--debug', $argv, true);

const VER = 'v3.34-worker-clean-smslog';

$LOG  = '/var/log/ucrm/worker.log';
$ELOG = '/var/log/ucrm/email.log';

function wlog(string $level, array $ctx=[]): void {
  $row = ['ts'=>date('c'),'lvl'=>$level,'ver'=>VER] + $ctx;
  @file_put_contents($GLOBALS['LOG'], json_encode($row, JSON_UNESCAPED_UNICODE).PHP_EOL, FILE_APPEND);
}
function email_log(array $ctx): void {
  $row = ['ts'=>date('c'),'lvl'=>'EMAIL','ver'=>VER] + $ctx;
  @file_put_contents($GLOBALS['ELOG'], json_encode($row, JSON_UNESCAPED_UNICODE).PHP_EOL, FILE_APPEND);
}

/** lock */
function open_lock_worker(): mixed {
  $paths = ['/run/lock/ucrm_worker.lock', '/tmp/ucrm_worker.lock'];
  foreach ($paths as $p) {
    $h = @fopen($p, 'c');
    if ($h && @flock($h, LOCK_EX | LOCK_NB)) { @chmod($p, 0666); return $h; }
  }
  return null;
}

function is_opted_out(PDO $pdo, string $u, string $ch): bool {
  $st=$pdo->prepare("SELECT 1 FROM crm_optout WHERE username=:u AND channel=:ch LIMIT 1");
  $st->execute([':u'=>$u, ':ch'=>$ch]);
  return (bool)$st->fetchColumn();
}
function fetch_nome_from_oracle(string $username): string {
  try {
    $r = oci_one("SELECT NOME FROM TRADER.AGC_CLIENTES WHERE TO_CHAR(CODIGOCLIENTE)=:U", [':U'=>$username]);
    return trim((string)($r['NOME'] ?? ''));
  } catch(Throwable $e) { return ''; }
}

/** Render placeholders + unsubscribe (email) */
function render_vars(?string $s, string $username, ?string $promo, ?string $nome, string $channel, ?string $ref_url=null): ?string {
  if ($s===null) return null;
  $out = str_replace(
    ['{{username}}','{{promo_code}}','{{nome}}','{{ref_url}}'],
    [$username, (string)($promo ?? ''), (string)($nome ?? ''), (string)($ref_url ?? '')],
    $s
  );
  if ($channel==='email' && strpos($out,'{{unsubscribe_url}}')!==false) {
    $unsub = build_unsub_url($username, 'email');
    $out = str_replace('{{unsubscribe_url}}', $unsub, $out);
  }
  return $out;
}

/** Throttling por campanha (RPM) */
function throttle_campaign(?int $cid, PDO $pdo): void {
  static $last = []; static $rate = [];
  if (!$cid) return;
  if (!isset($rate[$cid])) {
    $st = $pdo->prepare("SELECT send_rate_per_min FROM crm_campaigns WHERE id=:id");
    $st->execute([':id'=>$cid]);
    $rpm = (int)($st->fetchColumn() ?: 600);
    $rate[$cid] = max(1, $rpm);
  }
  $interval = 60.0 / $rate[$cid];
  $now = microtime(true);
  $next = ($last[$cid] ?? 0) + $interval;
  $sleep = $next - $now;
  if ($sleep > 0) { usleep((int)round($sleep*1e6)); wlog('THROTTLE', ['cid'=>$cid,'slept_ms'=>(int)round($sleep*1000)]); }
  $last[$cid] = microtime(true);
}

// ===== Execução =====
$lock = open_lock_worker();
if (!$lock) { wlog('SKIP', ['why'=>'locked']); exit; }

$pdo = pg_pdo();
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

$processed = 0;

while (true) {
  // Recolhe 1 item (reduce lock time)
  $pdo->beginTransaction();
  $st = $pdo->prepare("
    SELECT id, campaign_id, username, channel, address, payload, attempts
      FROM crm_outbox
     WHERE status='queued'
     ORDER BY id ASC
     LIMIT 1
     FOR UPDATE SKIP LOCKED
  ");
  $st->execute();
  $it = $st->fetch(PDO::FETCH_ASSOC);
  if (!$it) { $pdo->commit(); break; }

  $processed++;
  $id   = (int)$it['id'];
  $cid  = isset($it['campaign_id']) && $it['campaign_id']!==null ? (int)$it['campaign_id'] : null;
  $user = trim((string)$it['username']);
  $ch   = (string)$it['channel'];
  $addr = (string)($it['address'] ?? '');
  $payload = json_decode($it['payload'] ?? '{}', true) ?: [];

  // Normaliza payload (subject/body unificados)
  $payload = is_array($payload) ? $payload : (array)$payload;
  $payload = ucrm_normalize_payload($payload);

  // attempts ++
  $pdo->prepare("UPDATE crm_outbox SET attempts=attempts+1 WHERE id=:id")->execute([':id'=>$id]);
  $pdo->commit();

  try {
    throttle_campaign($cid, $pdo);

    // Opt-out (defensivo)
    if ($user!=='' && in_array($ch,['email','sms','push'],true) && is_opted_out($pdo,$user,$ch)) {
      $pdo->beginTransaction();
      $pdo->prepare("UPDATE crm_outbox SET status='skipped', last_error='optout' WHERE id=:id")->execute([':id'=>$id]);
      $pdo->commit();
      wlog('SKIP', ['id'=>$id,'u'=>$user,'ch'=>$ch,'cid'=>$cid,'why'=>'optout']);
      continue;
    }

    // Nome (só se necessário)
    $needNome=false;
    foreach (['subject','title','body','body_html','text','body_text'] as $k) {
      if (!empty($payload[$k]) && strpos((string)$payload[$k],'{{nome}}')!==false) { $needNome=true; break; }
    }
    $nome = $needNome ? fetch_nome_from_oracle($user) : null;

    // Referral URL (se exigido pelo template ou já vier no payload)
    $ref_url = $payload['ref_url'] ?? null;
    if ($ref_url === null) {
      $needRefUrl = false;
      foreach (['subject','title','body','body_html','text','body_text'] as $k) {
        if (!empty($payload[$k]) && strpos((string)$payload[$k],'{{ref_url}}') !== false) { $needRefUrl = true; break; }
      }
      if ($needRefUrl) {
        try {
          $ref_url = ref_build_url($user, $ch);
        } catch (Throwable $e) {
          $ref_url = ''; // não parte o render
          wlog('WARN', ['id'=>$id,'u'=>$user,'ch'=>$ch,'cid'=>$cid,'why'=>'ref_build_fail','err'=>$e->getMessage()]);
        }
      }
    }

    // Promo (se veio do scheduler)
    $promo = $payload['promo_code'] ?? null;

    $ok=false; $resp=''; $http=0;

    if ($ch==='email') {
      // to: preferir address (outbox.address); caso contrário payload.to
      $addr_src = null;
      $to = '';
      if ($addr !== '') { $to = $addr; $addr_src = 'outbox.address'; }
      elseif (!empty($payload['to'])) { $to = trim((string)$payload['to']); $addr_src = 'payload.to'; }

      if (!rt_email_is_whitelisted($to)) {
        $pdo->beginTransaction();
        $pdo->prepare("UPDATE crm_outbox SET status='skipped', last_error='whitelist' WHERE id=:id")->execute([':id'=>$id]);
        $pdo->commit();
        wlog('SKIP', ['id'=>$id,'u'=>$user,'ch'=>$ch,'cid'=>$cid,'why'=>'whitelist','to'=>$to,'addr_src'=>$addr_src]);
        email_log(['id'=>$id,'cid'=>$cid,'u'=>$user,'to'=>$to,'subject'=>($payload['subject']??null),'ok'=>false,'note'=>'blocked by whitelist','addr_src'=>$addr_src]);
        continue;
      }

      $from = $payload['from'] ?? (defined('SMTP_FROM') ? SMTP_FROM : 'no-reply@realtransfer.pt');

      $subject = (string)($payload['subject'] ?? ($payload['title'] ?? 'RealTransfer'));
      $subject = preg_replace('/\s*\|{2,}\s*/', ' | ', $subject);
      $subject = trim($subject, " \t|-");

      $html = (string)($payload['body_html'] ?? ((isset($payload['body']) && preg_match('/<\w[^>]*>/', (string)$payload['body'])) ? (string)$payload['body'] : ''));
      $text = (string)($payload['body_text'] ?? ($payload['text'] ?? strip_tags($html)));

      $subject = render_vars($subject, $user, $promo, $nome, 'email', $ref_url) ?? '';
      $html    = $html!=='' ? (render_vars($html, $user, $promo, $nome, 'email', $ref_url) ?? '') : '';
      $text    = render_vars($text, $user, $promo, $nome, 'email', $ref_url) ?? '';

      $ok = rt_sendmail($to, $subject, $html !== '' ? $html : null, $text, $from);
      $resp = $ok ? 'OK sendmail' : 'sendmail failed';

      email_log([
        'id'=>$id,'cid'=>$cid,'u'=>$user,
        'to'=>$to,'subject'=>$subject,
        'ok'=>$ok,'note'=>$resp,'mailer'=>'sendmail',
        'addr_src'=>$addr_src
      ]);

    } elseif ($ch==='sms') {
      $to   = $addr ?: '';
      if ($to === '') {
        $pdo->beginTransaction();
        $pdo->prepare("UPDATE crm_outbox SET status='skipped', last_error='noaddr' WHERE id=:id")->execute([':id'=>$id]);
        $pdo->commit();
        wlog('SKIP', ['id'=>$id,'u'=>$user,'ch'=>$ch,'cid'=>$cid,'why'=>'noaddr']);
        continue;
      }
      $text = render_vars($payload['text'] ?? ($payload['body_text'] ?? ''), $user, $promo, $nome, $ch, $ref_url) ?? '';
      $r = send_sms($to, $text);
      $ok = (bool)$r['ok']; $http = (int)($r['http'] ?? 0);
      $resp = (($r['error']??'').' '.($r['body']??'')) ?: '';

    } elseif ($ch==='push') {
      // Tokens: se address vier vazio, worker vai buscar a crm_devices
      $tokens = [];
      if ($addr!=='') $tokens[] = $addr;
      else {
        $t = $pdo->prepare("SELECT fcm_token FROM crm_devices WHERE username=:u ORDER BY created_at DESC LIMIT 10");
        $t->execute([':u'=>$user]);
        $tokens = array_map('strval', $t->fetchAll(PDO::FETCH_COLUMN));
      }
      if (!$tokens) { throw new RuntimeException('sem token push'); }

      $title = render_vars($payload['title'] ?? ($payload['subject'] ?? ''), $user, $promo, $nome, $ch, $ref_url) ?? '';
      $body  = render_vars($payload['text']  ?? ($payload['body'] ?? ''),     $user, $promo, $nome, $ch, $ref_url) ?? '';
      $r = fcm_send($tokens, $title, $body, ['username'=>$user,'campaign_id'=>$cid]);
      $ok = (bool)$r['ok']; $http = (int)($r['http'] ?? 0); $resp = (($r['error']??'').' '.($r['body']??'')) ?: '';

    } else {
      throw new RuntimeException('canal desconhecido: '.$ch);
    }

    // Persistir resultado
    $pdo->beginTransaction();
    if ($ok) {
      $pdo->prepare("UPDATE crm_outbox SET status='sent', sent_at=NOW(), last_error=NULL WHERE id=:id")->execute([':id'=>$id]);
      $pdo->commit();
      wlog('SENT', ['id'=>$id,'u'=>$user,'ch'=>$ch,'cid'=>$cid,'http'=>$http,'note'=>$resp]);
    } else {
      $pdo->prepare("UPDATE crm_outbox SET status='failed', last_error=:e WHERE id=:id")
          ->execute([':id'=>$id, ':e'=>mb_substr(trim($resp?:('HTTP '.$http)),0,800)]);
      $pdo->commit();
      wlog('FAIL', ['id'=>$id,'u'=>$user,'ch'=>$ch,'cid'=>$cid,'http'=>$http,'err'=>$resp]);
    }

  } catch (Throwable $e) {
    // Inesperado
    $pdo->beginTransaction();
    $pdo->prepare("UPDATE crm_outbox SET status='failed', last_error=:e WHERE id=:id")
        ->execute([':id'=>$id, ':e'=>mb_substr($e->getMessage(),0,800)]);
    $pdo->commit();
    wlog('EXC', ['id'=>$id,'u'=>$user,'ch'=>$ch,'cid'=>$cid,'err'=>$e->getMessage()]);
  }
}

wlog('DONE', ['processed'=>$processed]);

