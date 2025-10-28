<?php
require_once __DIR__ . '/../inc/db_pg.php';

$pdo = pg_pdo();

// Marca como 'redeemed' onde a promotion_code já tem used_at
$sql = "
UPDATE crm_promotions_assigned cpa
SET status='redeemed', redeemed_at = pc.used_at
FROM public.promotion_code pc
WHERE LOWER(cpa.promo_code) = LOWER(pc.code)
  AND pc.used_at IS NOT NULL
  AND cpa.status <> 'redeemed';
";
$aff = $pdo->exec($sql);
echo "[redeem_sync] updated rows: ", (int)$aff, PHP_EOL;
