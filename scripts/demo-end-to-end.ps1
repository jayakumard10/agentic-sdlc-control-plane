<#
.SYNOPSIS
    Drive one full signal through the running platform, end to end.

.DESCRIPTION
    Assumes the platform is already up and healthy (demo-platform.ps1). Then:

      1. Real traffic against the tenant service, which publishes request telemetry.
      2. Confirms that telemetry actually landed on the broker.
      3. Publishes a drift signal.
      4. Watches the control plane consume it, clone the target, and park at a human gate.
      5. Answers each gate with a decision event.
      6. Reads the run-outcome event back off the broker.

    Step 3 is a deliberate shortcut, and the only one. Genuine drift detection compares a
    trailing 7-day reference window against the current hour, which cannot be populated inside a
    demo. Everything downstream of the drift event is the real production path: real pattern
    subscription, real clone, real LangGraph interrupt, real durable resume from Postgres.

.PARAMETER Root
    Directory containing the four repositories. Defaults to the parent of this repository.

.PARAMETER PauseSec
    Pause between narrated steps, so a screen recording is readable. Default 2.
#>

[CmdletBinding()]
param(
    [string] $Root,
    [int]    $PauseSec = 2
)

# See demo-platform.ps1 for why this is Continue: docker writes normal progress to stderr, which
# PowerShell 5.1 turns into a terminating error under 'Stop'.
$ErrorActionPreference = 'Continue'

if (-not $Root) { $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot) }

$EventbusNetwork  = 'eventbus'
$KafkaClientImage = 'apache/kafka:4.1.2'
$BrokerInternal   = 'broker:19092'
$TenantBase       = 'http://localhost:8000'
$TenantApiKey     = 'dev-local-api-key'   # url_shortener/auth.py dev default; not overridden in compose
$ControlPlaneLog  = 'agentic-sdlc-control-plane-consumer'

$RunId = "demo-{0}" -f (Get-Date -Format 'yyyyMMdd-HHmmss')

function Write-Banner { param([string]$T)
    Write-Host ''
    Write-Host ('=' * 78) -ForegroundColor DarkMagenta
    Write-Host "  $T" -ForegroundColor Magenta
    Write-Host ('=' * 78) -ForegroundColor DarkMagenta
}
function Write-Step { param([string]$T) Write-Host ''; Write-Host ("  >> {0}" -f $T) -ForegroundColor White }
function Write-Info { param([string]$T) Write-Host ("     {0}" -f $T) -ForegroundColor Gray }
function Write-Ok   { param([string]$T) Write-Host ("     OK   {0}" -f $T) -ForegroundColor Green }
function Write-Warn2{ param([string]$T) Write-Host ("     WARN {0}" -f $T) -ForegroundColor Yellow }
function Write-Fail { param([string]$T) Write-Host ("     FAIL {0}" -f $T) -ForegroundColor Red }
function Pause-Demo { Start-Sleep -Seconds $PauseSec }

function Publish-Event {
    param([string] $Topic, [string] $Json)
    # The JSON is handed over on stdin rather than as an argument, so quoting never has to
    # survive PowerShell, cmd and sh in sequence.
    $tmp = [IO.Path]::GetTempFileName()
    try {
        [IO.File]::WriteAllText($tmp, $Json)
        $cmd = "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BrokerInternal --topic $Topic"
        & cmd /c "type ""$tmp"" | docker run -i --rm --network $EventbusNetwork $KafkaClientImage sh -c ""$cmd"" 2>&1" | Out-Null
        return ($LASTEXITCODE -eq 0)
    } finally {
        Remove-Item $tmp -ErrorAction SilentlyContinue
    }
}

function Read-Events {
    param([string] $Topic, [int] $Max = 1, [int] $TimeoutMs = 15000)
    $cmd = "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BrokerInternal --topic $Topic --from-beginning --max-messages $Max --timeout-ms $TimeoutMs"
    # The consumer exits non-zero on its timeout even after printing messages, so the exit code
    # is deliberately ignored here and the caller judges by what came back.
    $out = & cmd /c "docker run --rm --network $EventbusNetwork $KafkaClientImage sh -c ""$cmd"" 2>nul"
    return $out
}

function Wait-ForLog {
    param([string] $Pattern, [int] $TimeoutSec = 90, [string] $Describe)
    $sw = [Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $TimeoutSec) {
        $logs = (& cmd /c "docker logs $ControlPlaneLog 2>&1") -join "`n"
        if ($logs -match $Pattern) {
            Write-Ok ("{0} ({1:N0}s)" -f $Describe, $sw.Elapsed.TotalSeconds)
            return $true
        }
        Start-Sleep -Seconds 2
    }
    Write-Fail ("timed out after {0}s waiting for: {1}" -f $TimeoutSec, $Describe)
    return $false
}

function New-Envelope {
    param([string] $EventType, [string] $Service, [string] $CorrelationId, [string] $RepoUrl,
          [hashtable] $Metrics, [hashtable] $Payload)
    $env = [ordered]@{
        schema_version = '1.0'
        event_id       = [guid]::NewGuid().ToString()
        correlation_id = $CorrelationId
        tenant         = 'default'
        service        = $Service
        event_type     = $EventType
        timestamp      = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
        producer       = [ordered]@{ service = $Service; instance_id = 'demo-script' }
        git_target     = [ordered]@{ repo_url = $RepoUrl; branch = 'main'; commit_sha = $null }
        scenario_type  = 'brownfield'
        metrics        = $Metrics
        payload        = $Payload
    }
    return ($env | ConvertTo-Json -Depth 6 -Compress)
}

# ---------------------------------------------------------------------------

Write-Banner 'END-TO-END DEMO'
Write-Info ("run id: {0}" -f $RunId)
Write-Info 'Every step below is the real production path except the drift signal itself,'
Write-Info 'which is injected because genuine detection needs a 7-day reference window.'

# --- 1. Real tenant traffic -------------------------------------------------

Write-Step '1/6  Real traffic against the tenant service'
$headers = @{ 'X-API-Key' = $TenantApiKey; 'Content-Type' = 'application/json' }
$code = $null
try {
    $body = @{ long_url = 'https://example.com/a-genuinely-long-url-for-the-demo' } | ConvertTo-Json
    $created = Invoke-RestMethod -Uri "$TenantBase/shorten" -Method Post -Headers $headers -Body $body -TimeoutSec 15
    $code = $created.code
    Write-Ok ("POST /shorten -> code '{0}'" -f $code)
} catch {
    Write-Fail ("tenant service did not accept the request: {0}" -f $_.Exception.Message)
    Write-Warn2 'Is the platform up? Run demo-platform.ps1 first.'
    exit 1
}

Write-Info 'generating redirect traffic (each request publishes a telemetry event)'
for ($i = 1; $i -le 8; $i++) {
    try { Invoke-WebRequest -Uri "$TenantBase/$code" -MaximumRedirection 0 -ErrorAction SilentlyContinue -TimeoutSec 10 | Out-Null } catch { }
}
Write-Ok '8 redirects issued'
Pause-Demo

# --- 2. Telemetry on the broker --------------------------------------------

Write-Step '2/6  Confirming telemetry reached the event bus'
$telemetry = Read-Events -Topic 'url-shortener.request-telemetry.v1' -Max 1 -TimeoutMs 20000
if ($telemetry) {
    Write-Ok 'request-telemetry event present on the broker'
    Write-Info (($telemetry | Select-Object -First 1).ToString().Substring(0, [Math]::Min(150, ($telemetry | Select-Object -First 1).ToString().Length)) + ' ...')
} else {
    Write-Warn2 'no telemetry read back - continuing, the drift signal below does not depend on it'
}
Pause-Demo

# --- 3. Drift signal --------------------------------------------------------

Write-Step '3/6  Publishing a drift signal  [the one injected step]'
$driftJson = New-Envelope -EventType 'drift-detected' -Service 'agentic-sdlc-mlops' `
    -CorrelationId $RunId `
    -RepoUrl 'https://github.com/jayakumard10/agentic-sdlc-eventbus.git' `
    -Metrics @{ metric_name = 'p95_latency_ms'; reference_value = 42.1; current_value = 61.8; relative_delta_pct = 46.8; threshold_pct = 20.0 } `
    -Payload @{ sample_size = 483 }

if (Publish-Event -Topic 'mlops.drift-detected.v1' -Json $driftJson) {
    Write-Ok ("drift-detected published, correlation_id = {0}" -f $RunId)
    Write-Info 'p95 latency 42.1ms -> 61.8ms (+46.8% against a 20% threshold)'
} else {
    Write-Fail 'could not publish the drift event'
    exit 1
}

# --- 4. Control plane reacts ------------------------------------------------

Write-Step '4/6  Control plane: consume, clone, and park at a human gate'
if (-not (Wait-ForLog -Pattern ([regex]::Escape($RunId) + '.*(Cloning|workspace ready)|Cloning.*') -TimeoutSec 90 -Describe 'cloned the target repository')) { exit 1 }
if (-not (Wait-ForLog -Pattern 'parked at gate' -TimeoutSec 60 -Describe 'run parked at a human gate')) { exit 1 }
Write-Info 'The poll loop did not block here - the run lives in Postgres, not in the consumer.'
& cmd /c "docker logs --tail 6 $ControlPlaneLog 2>&1" | Select-String -Pattern 'agentic_control_plane' | ForEach-Object { Write-Host ("       {0}" -f $_.Line) -ForegroundColor DarkGray }
Pause-Demo

# --- 5. Human decisions -----------------------------------------------------

Write-Step '5/6  Answering the gates as a human reviewer would'
for ($g = 1; $g -le 3; $g++) {
    $decisionJson = New-Envelope -EventType 'gate-decision' -Service 'agentic-sdlc-control-plane' `
        -CorrelationId $RunId `
        -RepoUrl 'https://github.com/jayakumard10/agentic-sdlc-eventbus.git' `
        -Metrics @{} `
        -Payload @{ gate_type = 'any'; decision = 'approve'; decided_by = 'demo-reviewer'; comment = 'Approved during the end-to-end demo.' }

    Publish-Event -Topic 'control-plane.gate-decision.v1' -Json $decisionJson | Out-Null
    Write-Ok ("decision #{0} published (approve, by demo-reviewer)" -f $g)

    Start-Sleep -Seconds 6
    $logs = (& cmd /c "docker logs $ControlPlaneLog 2>&1") -join "`n"
    if ($logs -match 'reached terminal state') { break }
}

# --- 6. Outcome -------------------------------------------------------------

Write-Step '6/6  Run outcome'
if (Wait-ForLog -Pattern 'reached terminal state' -TimeoutSec 60 -Describe 'run reached a terminal state') {
    & cmd /c "docker logs --tail 12 $ControlPlaneLog 2>&1" | Select-String -Pattern 'terminal state|Committed|Removed workspace' | ForEach-Object { Write-Host ("       {0}" -f $_.Line) -ForegroundColor DarkGray }
}

Write-Info 'reading the outcome event back off the broker'
$outcomes = Read-Events -Topic 'control-plane.run-outcome.v1' -Max 50 -TimeoutMs 15000
$mine = $outcomes | Where-Object { $_ -match [regex]::Escape($RunId) }
if ($mine) {
    Write-Ok 'run-outcome event published'
    Write-Host ''
    Write-Host ($mine | Select-Object -First 1) -ForegroundColor DarkGray
} else {
    Write-Warn2 'outcome event not found on the topic within the timeout'
}

Write-Banner 'DEMO COMPLETE'
Write-Host '  Signal path exercised:' -ForegroundColor White
Write-Host '    tenant request -> telemetry -> drift -> clone -> gate -> decision -> outcome' -ForegroundColor Gray
Write-Host ''
Write-Host '  Everything except the injected drift signal was the real production path,' -ForegroundColor Gray
Write-Host '  including a durable interrupt that survives the process that created it.' -ForegroundColor Gray
Write-Host ''
