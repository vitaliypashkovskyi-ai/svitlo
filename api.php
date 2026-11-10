<?php
// Svitlo Kyiv — address lookup proxy for QNAP Web Server (PHP).
// The browser can't call YASNO directly (no CORS headers), so this forwards
// only three read-only lookups: streets, houses, group. ?ep=ping checks it works.

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');

const YASNO = 'https://app.yasno.ua/api/blackout-service/public/shutdowns/addresses/v2';
const REGION_ID = '25';   // Київ
const DSO_ID = '902';     // ДТЕК Київські електромережі
$routes = [
    'streets' => ['query'],
    'houses'  => ['streetId', 'query'],
    'group'   => ['streetId', 'houseId'],
];

function out($code, $obj) {
    http_response_code($code);
    echo json_encode($obj, JSON_UNESCAPED_UNICODE);
    exit;
}

$ep = $_GET['ep'] ?? '';
if ($ep === 'ping') out(200, ['ok' => true]);
if (!isset($routes[$ep])) out(404, ['error' => 'unknown endpoint']);

$params = ['regionId' => REGION_ID, 'dsoId' => DSO_ID];
foreach ($routes[$ep] as $key) {
    $val = mb_substr(trim((string)($_GET[$key] ?? '')), 0, 100);
    if ($val === '') out(400, ['error' => "missing $key"]);
    $params[$key] = $val;
}

$ch = curl_init(YASNO . '/' . $ep . '?' . http_build_query($params));
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_TIMEOUT => 15,
    CURLOPT_HTTPHEADER => ['Accept: application/json'],
    CURLOPT_USERAGENT => 'svitlo-kyiv-qnap/1.0',
]);
$body = curl_exec($ch);
$status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$err = curl_error($ch);
curl_close($ch);

if ($body === false || $status !== 200 || json_decode($body) === null) {
    out(502, ['error' => 'YASNO unavailable' . ($err ? ": $err" : " (HTTP $status)")]);
}
echo $body;
