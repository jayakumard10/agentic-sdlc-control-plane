<#
.SYNOPSIS
    Launch the whole agentic-sdlc platform locally, in dependency order, with health gating.

.DESCRIPTION
    Brings up all four repositories' Docker Compose stacks as one platform, waiting on a real
    readiness signal between stages rather than a fixed sleep.

    ORDERING - what is a genuine dependency and what is not, because guessing here is what
    produces flaky demos:

      1. agentic-sdlc-eventbus MUST be first and MUST be healthy before anything else starts.
         Every other service opens a Kafka connection during startup. Against a broker that is
         not yet listening, the clients enter a retry/backoff cycle that can delay group join
         and topic discovery far past the point where the rest of the demo is waiting on them.

      2. url-shortener-api, agentic-sdlc-mlops and agentic-sdlc-control-plane have no hard
         dependency on each other. They are ordered producer-before-consumer here for a timing
         reason, not a correctness one: both consumers subscribe by PATTERN, and a pattern
         subscriber only discovers a newly created topic on its next metadata refresh
         (metadata.max.age.ms, tuned to 30s). Starting a consumer before its topic exists costs
         up to 30 seconds of dead air.

         This script removes that wait entirely by pre-creating the four platform topics before
         any consumer starts (see Stage 1b), so partitions are assigned immediately. That is a
         demo convenience. In production, auto-creation plus the metadata refresh handles it,
         and the 30s is irrelevant because nothing is watching.

    Each stage waits on the strongest signal the service actually exposes. Every container now
    reports health, so that is always one of them. The two consumer services additionally wait
    on a readiness line in their logs, because their healthcheck and their log line prove
    different things: the healthcheck says the background loop is alive, the log line says the
    Kafka client has joined its group and can actually receive. Neither implies the other.

.PARAMETER Root
    Directory containing all four repository folders. Defaults to the parent of this repository.

.PARAMETER Build
    Rebuild images before starting. Needed on first run, or after code changes. No credential is
    required to build: the shared agentic-events package installs from a public repository.

.PARAMETER Demo
    After everything is healthy, drive a full end-to-end run: real traffic to the tenant
    service, a drift signal, a control-plane run, a human gate decision, and the outcome event.

.PARAMETER Down
    Stop and remove all four stacks, then exit.

.PARAMETER Status
    Print current status of all platform containers, then exit.

.PARAMETER TimeoutSec
    Per-service readiness timeout. Default 180.

.EXAMPLE
    .\scripts\demo-platform.ps1 -Build
    First run: build all images, then bring the platform up in order.

.EXAMPLE
    .\scripts\demo-platform.ps1 -Demo
    Bring the platform up and run the end-to-end demo. This is the one to record.

.EXAMPLE
    .\scripts\demo-platform.ps1 -Down
    Tear everything down.

.NOTES
    Requires all four repositories checked out side by side under -Root. That is a prerequisite
    of this script only - each repository still starts on its own with no sibling checkout, and
    nothing in any repository's own compose file references another.

    ON THE DOMAIN-AGNOSTIC RULE: this script, and demo-end-to-end.ps1 beside it, deliberately
    know about all four repositories by name, including tenant-specific details such as the
    tenant API's endpoints and its development API key. That is unavoidable - a demo of a
    platform requires a concrete tenant to demo it against - and it is precisely why these live
    in scripts/ rather than in agentic_control_plane/.

    The boundary is the package, not the repository. Nothing under agentic_control_plane/ or
    tests/ may reference a tenant concept, and CI enforces exactly that scope on every push
    (see .github/workflows/ci.yml). Demo tooling sits outside that boundary by design. If these
    scripts were ever imported by the package, the rule would be broken - they are not, and must
    not be.
#>

[CmdletBinding()]
param(
    [string] $Root,
    [switch] $Build,
    [switch] $Demo,
    [switch] $Down,
    [switch] $Status,
    [int]    $TimeoutSec = 180
)

# Continue, not Stop, and deliberately so. Windows PowerShell 5.1 surfaces a native
# executable's stderr as a NativeCommandError, and `docker compose` writes all of its normal
# progress output ("Container ... Stopping") to stderr. Under 'Stop' the script dies on a
# perfectly successful teardown. Every docker invocation here is checked through $LASTEXITCODE
# instead, which is the reliable signal, and cmdlet failures that matter are wrapped in try/catch.
$ErrorActionPreference = 'Continue'

# ---------------------------------------------------------------------------
# Platform definition
# ---------------------------------------------------------------------------

if (-not $Root) {
    $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$EventbusNetwork = 'eventbus'
$KafkaClientImage = 'apache/kafka:4.1.2'
$BrokerInternal = 'broker:19092'

# Topics pre-created so pattern subscribers get partitions immediately. Order of this list is
# irrelevant; the convention is {service}.{event-type}.v{n}.
$PlatformTopics = @(
    'url-shortener.request-telemetry.v1',
    'mlops.drift-detected.v1',
    'control-plane.gate-decision.v1',
    'control-plane.run-outcome.v1',
    'control-plane.audit.v1'
)

# Stage table. Each service declares how to know it is actually ready, not merely started.
#   Health : wait for the container's own healthcheck to report healthy
#   Log    : wait for a regex to appear in the container's logs
#
# The two are not interchangeable and the consumer services wait on both. A healthcheck here
# reports that the service's background loop is turning; a log line reports that its Kafka
# client has joined a group and can receive. A container can satisfy either while failing the
# other, so taking one as evidence of the other is how a demo starts against a service that
# cannot yet hear it.
$Stages = @(
    @{
        Name  = 'agentic-sdlc-eventbus'
        Repo  = 'agentic-sdlc-eventbus'
        Why   = 'Message broker. Everything else connects to it during startup.'
        Waits = @(
            @{ Container = 'agentic-sdlc-eventbus'; Kind = 'Health' }
        )
    },
    @{
        Name  = 'url-shortener-api'
        Repo  = 'url-shortener-api'
        Why   = 'Tenant service. Produces the telemetry the platform reacts to.'
        Waits = @(
            @{ Container = 'url-shortener-postgres'; Kind = 'Health' },
            @{ Container = 'url-shortener-api';      Kind = 'Health' }
        )
    },
    @{
        Name  = 'agentic-sdlc-mlops'
        Repo  = 'agentic-sdlc-mlops'
        Why   = 'Drift detection. Consumes telemetry, publishes drift events.'
        Waits = @(
            @{ Container = 'agentic-sdlc-mlops-mlflow';   Kind = 'Health' },
            # Health says the drift-check loop is alive (docs/adr/0008 in that repo). It says
            # nothing about ingestion, so the group join is still waited on separately - that
            # is the point at which this service can actually receive anything.
            @{ Container = 'agentic-sdlc-mlops-consumer'; Kind = 'Health' },
            @{ Container = 'agentic-sdlc-mlops-consumer'; Kind = 'Log'
               Pattern = 'Successfully joined group|Subscribing to pattern' }
        )
    },
    @{
        Name  = 'agentic-sdlc-control-plane'
        Repo  = 'agentic-sdlc-control-plane'
        Why   = 'Orchestrator. Consumes drift events, runs the governed workflow, gates on a human.'
        Waits = @(
            @{ Container = 'agentic-sdlc-control-plane-postgres'; Kind = 'Health' },
            # Health says the worker thread is turning (docs/adr/0013). The readiness line says
            # the trigger poll loop is subscribed. A run needs both: one to accept the work, the
            # other to execute it.
            @{ Container = 'agentic-sdlc-control-plane-consumer'; Kind = 'Health' },
            @{ Container = 'agentic-sdlc-control-plane-consumer'; Kind = 'Log'
               Pattern = 'Control plane ready' }
        )
    }
)

# Secrets each repository needs present before compose will start. Real values are gitignored;
# only .example templates are tracked, so a fresh clone always hits this check.
$RequiredSecrets = @{
    'url-shortener-api'          = @('secrets/postgres_password.txt', 'secrets/github_pat.txt')
    'agentic-sdlc-mlops'         = @('secrets/github_pat.txt')
    'agentic-sdlc-control-plane' = @('secrets/postgres_password.txt', 'secrets/github_pat.txt')
}

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

$script:StageNumber = 0

function Write-Banner {
    param([string] $Text)
    Write-Host ''
    Write-Host ('=' * 78) -ForegroundColor DarkCyan
    Write-Host "  $Text" -ForegroundColor Cyan
    Write-Host ('=' * 78) -ForegroundColor DarkCyan
}

function Write-Stage {
    param([string] $Name, [string] $Why)
    $script:StageNumber++
    Write-Host ''
    Write-Host ("[{0}] {1}" -f $script:StageNumber, $Name) -ForegroundColor White
    Write-Host ("     {0}" -f $Why) -ForegroundColor DarkGray
}

function Write-Step   { param([string] $Text) Write-Host ("     -> {0}" -f $Text) -ForegroundColor Gray }
function Write-Ok     { param([string] $Text) Write-Host ("     OK   {0}" -f $Text) -ForegroundColor Green }
function Write-Warn2  { param([string] $Text) Write-Host ("     WARN {0}" -f $Text) -ForegroundColor Yellow }
function Write-Fail   { param([string] $Text) Write-Host ("     FAIL {0}" -f $Text) -ForegroundColor Red }

function Stop-WithError {
    param([string] $Message, [string] $Hint)
    Write-Host ''
    Write-Fail $Message
    if ($Hint) { Write-Host ("     {0}" -f $Hint) -ForegroundColor Yellow }
    Write-Host ''
    exit 1
}

# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------

# All docker invocations go through cmd.exe, which merges stderr into stdout before PowerShell
# sees it. Without that, PowerShell 5.1 wraps each native stderr line in an ErrorRecord and
# prints a NativeCommandError block - and `docker compose` writes all of its ordinary progress
# ("Container X Stopping") to stderr, so a completely successful run produces pages of red.
# That matters here beyond tidiness: this output is meant to be watched, and recorded.
$script:LastDockerExit = 0

function Invoke-DockerCli {
    param([string] $ArgLine)
    $out = & cmd /c "docker $ArgLine 2>&1"
    $script:LastDockerExit = $LASTEXITCODE
    return $out
}

function Invoke-Compose {
    param([string] $RepoPath, [string[]] $ComposeArgs)
    Push-Location $RepoPath
    try {
        $out = Invoke-DockerCli -ArgLine ("compose " + ($ComposeArgs -join ' '))
        # Compose chatter is suppressed while it succeeds and surfaced in full when it does not,
        # so a failure is never silent but a success stays readable.
        if ($script:LastDockerExit -ne 0) {
            foreach ($line in $out) { Write-Host ("       {0}" -f $line) -ForegroundColor DarkGray }
        }
        return $script:LastDockerExit
    } finally {
        Pop-Location
    }
}

function Get-ContainerState {
    param([string] $Name)
    # The Go template lives in a variable rather than inline: PowerShell's -f operator reads
    # `{{` as an escaped literal brace, so an inline "{{.State.Status}}" is silently rewritten
    # to "{.State.Status}" and docker returns that text verbatim instead of the value.
    $fmt = "{{.State.Status}}"
    $state = Invoke-DockerCli -ArgLine ("inspect --format ""{0}"" {1}" -f $fmt, $Name)
    if ($script:LastDockerExit -ne 0) { return $null }
    return ($state | Select-Object -First 1)
}

function Get-ContainerHealth {
    param([string] $Name)
    # Containers without a healthcheck have no .State.Health at all; report that as 'none' so a
    # caller can tell "no signal available" apart from "signal says unhealthy".
    $fmt = "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}"
    $health = Invoke-DockerCli -ArgLine ("inspect --format ""{0}"" {1}" -f $fmt, $Name)
    if ($script:LastDockerExit -ne 0) { return $null }
    return ($health | Select-Object -First 1)
}

function Get-ContainerLogs {
    param([string] $Name, [int] $Tail = 0)
    if ($Tail -gt 0) { return Invoke-DockerCli -ArgLine ("logs --tail {0} {1}" -f $Tail, $Name) }
    return Invoke-DockerCli -ArgLine ("logs {0}" -f $Name)
}

function Wait-ContainerHealthy {
    param([string] $Name, [int] $Timeout)
    $sw = [Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $Timeout) {
        $state = Get-ContainerState -Name $Name
        if ($state -eq 'exited' -or $state -eq 'dead') {
            Write-Fail ("{0} exited before becoming healthy" -f $Name)
            Get-ContainerLogs -Name $Name -Tail 25 | ForEach-Object { Write-Host ("       {0}" -f $_) -ForegroundColor DarkGray }
            return $false
        }
        $health = Get-ContainerHealth -Name $Name
        if ($health -eq 'healthy') {
            Write-Ok ("{0,-42} healthy in {1:N0}s" -f $Name, $sw.Elapsed.TotalSeconds)
            return $true
        }
        Start-Sleep -Seconds 2
    }
    Write-Fail ("{0} did not become healthy within {1}s" -f $Name, $Timeout)
    Get-ContainerLogs -Name $Name -Tail 25 | ForEach-Object { Write-Host ("       {0}" -f $_) -ForegroundColor DarkGray }
    return $false
}

function Wait-ContainerLog {
    param([string] $Name, [string] $Pattern, [int] $Timeout)
    $sw = [Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $Timeout) {
        $state = Get-ContainerState -Name $Name
        if ($state -eq 'exited' -or $state -eq 'dead') {
            Write-Fail ("{0} exited before signalling readiness" -f $Name)
            Get-ContainerLogs -Name $Name -Tail 25 | ForEach-Object { Write-Host ("       {0}" -f $_) -ForegroundColor DarkGray }
            return $false
        }
        # stderr is where these services log; docker logs merges both streams for us.
        $logs = (Get-ContainerLogs -Name $Name) -join "`n"
        if ($logs -match $Pattern) {
            Write-Ok ("{0,-42} ready in {1:N0}s" -f $Name, $sw.Elapsed.TotalSeconds)
            return $true
        }
        Start-Sleep -Seconds 2
    }
    Write-Fail ("{0} never logged /{1}/ within {2}s" -f $Name, $Pattern, $Timeout)
    Get-ContainerLogs -Name $Name -Tail 25 | ForEach-Object { Write-Host ("       {0}" -f $_) -ForegroundColor DarkGray }
    return $false
}

function Invoke-KafkaCli {
    param([string[]] $CliArgs)
    # A disposable JVM Kafka client on the broker's own network. The broker image itself is
    # kafka-native and ships no CLI tooling, so admin commands need this sidecar.
    $joined = $CliArgs -join ' '
    return Invoke-DockerCli -ArgLine ("run --rm --network {0} {1} sh -c ""{2}""" -f $EventbusNetwork, $KafkaClientImage, $joined)
}

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

function Test-Preflight {
    Write-Banner 'PREFLIGHT'

    Write-Step 'Docker daemon'
    $ver = Invoke-DockerCli -ArgLine 'version --format "{{.Server.Version}}"'
    if ($script:LastDockerExit -ne 0) {
        Stop-WithError 'Docker is not reachable.' 'Start Docker Desktop and try again.'
    }
    Write-Ok ("docker engine {0}" -f ($ver | Select-Object -First 1))

    Write-Step ("Repositories under {0}" -f $Root)
    $missing = @()
    foreach ($stage in $Stages) {
        $path = Join-Path $Root $stage.Repo
        if (-not (Test-Path (Join-Path $path 'docker-compose.yml'))) { $missing += $stage.Repo }
    }
    if ($missing.Count -gt 0) {
        Stop-WithError ("Missing repositories: {0}" -f ($missing -join ', ')) `
            ("Clone all four side by side under $Root, or pass -Root <path>.")
    }
    Write-Ok ("all 4 repositories found")

    Write-Step 'Secrets'
    $missingSecrets = @()
    foreach ($repo in $RequiredSecrets.Keys) {
        foreach ($rel in $RequiredSecrets[$repo]) {
            $full = Join-Path (Join-Path $Root $repo) $rel
            if (-not (Test-Path $full)) { $missingSecrets += ("{0}/{1}" -f $repo, $rel) }
        }
    }
    if ($missingSecrets.Count -gt 0) {
        Write-Host ''
        foreach ($m in $missingSecrets) { Write-Fail ("missing {0}" -f $m) }
        Stop-WithError 'Required secret files are absent.' `
            'Copy each alongside its .example template and fill it in. The GitHub PAT is a runtime credential for clone-per-run: it needs read access to the tenant repositories a run will clone.'
    }
    Write-Ok 'all required secret files present'

    Write-Step 'Host ports'
    # A port conflict otherwise surfaces much later as an opaque compose failure. A port held by
    # this platform's own container is not a conflict, so the owning container is checked before
    # warning - re-running the launcher against a live platform should be quiet.
    $ports = @(
        @{ Port = 5000; Owner = 'agentic-sdlc-mlops-mlflow';           Label = 'mlflow' },
        @{ Port = 5433; Owner = 'agentic-sdlc-control-plane-postgres'; Label = 'control-plane postgres' },
        @{ Port = 8000; Owner = 'url-shortener-api';                   Label = 'tenant API' },
        @{ Port = 9092; Owner = 'agentic-sdlc-eventbus';               Label = 'eventbus (host listener)' },
        @{ Port = 9093; Owner = 'agentic-sdlc-eventbus';               Label = 'eventbus (container listener)' }
    )
    $conflicts = 0
    foreach ($p in $ports) {
        $inUse = $null
        try { $inUse = Get-NetTCPConnection -LocalPort $p.Port -State Listen -ErrorAction SilentlyContinue } catch { }
        if (-not $inUse) { continue }
        if ((Get-ContainerState -Name $p.Owner) -eq 'running') { continue }
        Write-Warn2 ("port {0} ({1}) is held by something that is not {2}" -f $p.Port, $p.Label, $p.Owner)
        $conflicts++
    }
    if ($conflicts -eq 0) { Write-Ok 'no port conflicts' }
    else { Write-Warn2 ("{0} port conflict(s) - compose will fail to bind" -f $conflicts) }
}

# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

function Invoke-Down {
    Write-Banner 'TEARING DOWN'
    # Reverse order: consumers before the broker they talk to, so nothing spends its shutdown
    # window retrying a connection to something already gone.
    $reversed = @($Stages)
    [array]::Reverse($reversed)
    foreach ($stage in $reversed) {
        $path = Join-Path $Root $stage.Repo
        if (-not (Test-Path $path)) { continue }
        Write-Step ("{0}" -f $stage.Name)
        Invoke-Compose -RepoPath $path -ComposeArgs @('down') | Out-Null
        Write-Ok ("{0} stopped" -f $stage.Name)
    }
    Write-Host ''
    Write-Host '  Volumes were preserved. Add -v to the compose down commands to drop data.' -ForegroundColor DarkGray
    Write-Host ''
}

function Show-Status {
    Write-Banner 'PLATFORM STATUS'
    # Deduplicated: a container may declare more than one readiness signal (health *and* a log
    # line), and it is still one container to report on.
    $names = @()
    foreach ($stage in $Stages) {
        foreach ($w in $stage.Waits) {
            if ($names -notcontains $w.Container) { $names += $w.Container }
        }
    }
    Write-Host ''
    Write-Host ("  {0,-44} {1,-12} {2}" -f 'CONTAINER', 'STATE', 'HEALTH') -ForegroundColor White
    Write-Host ("  {0}" -f ('-' * 72)) -ForegroundColor DarkGray
    foreach ($n in $names) {
        $state = Get-ContainerState -Name $n
        if (-not $state) { $state = 'absent' }
        $health = Get-ContainerHealth -Name $n
        if (-not $health) { $health = '-' }
        $colour = 'Gray'
        if ($state -eq 'running' -and ($health -eq 'healthy' -or $health -eq 'none')) { $colour = 'Green' }
        if ($state -eq 'absent' -or $state -eq 'exited') { $colour = 'Red' }
        Write-Host ("  {0,-44} {1,-12} {2}" -f $n, $state, $health) -ForegroundColor $colour
    }
    Write-Host ''
}

function New-PlatformTopics {
    Write-Stage 'Pre-create platform topics' 'Removes up to 30s of pattern-discovery lag before consumers start. Demo convenience, not a production requirement.'
    foreach ($topic in $PlatformTopics) {
        $cmd = "/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BrokerInternal --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1"
        $out = Invoke-KafkaCli -CliArgs @($cmd)
        if ($script:LastDockerExit -eq 0) {
            Write-Ok ("{0}" -f $topic)
        } else {
            Write-Warn2 ("could not pre-create {0} - auto-creation will still handle it, with the discovery delay" -f $topic)
        }
    }
}

function Start-Platform {
    Write-Banner 'LAUNCHING PLATFORM'

    foreach ($stage in $Stages) {
        $path = Join-Path $Root $stage.Repo
        Write-Stage $stage.Name $stage.Why

        $composeArgs = @()
        # The demo mounts a synthetic fixture into the control plane so the run reaches
        # `completed` rather than safe-stopping at the coder node. The shipped image has an
        # empty /fixtures on purpose (docs/adr/0001); this override is demo-only.
        if ($Demo -and $stage.Repo -eq 'agentic-sdlc-control-plane') {
            $override = Join-Path $path 'scripts/demo/compose.demo-fixtures.yml'
            if (Test-Path $override) {
                $composeArgs += @('-f', 'docker-compose.yml', '-f', 'scripts/demo/compose.demo-fixtures.yml')
                Write-Step 'applying demo fixtures override (enables replay mode for the demo)'
            } else {
                Write-Warn2 'demo fixtures override missing - the demo run will safe-stop at the coder node'
            }
        }
        $composeArgs += @('up', '-d')
        if ($Build) { $composeArgs += '--build' }

        Write-Step ("docker compose {0}" -f ($composeArgs -join ' '))
        $code = Invoke-Compose -RepoPath $path -ComposeArgs $composeArgs
        if ($code -ne 0) {
            Stop-WithError ("compose up failed for {0}" -f $stage.Name) `
                'Run it directly in that repository to see the full output.'
        }

        foreach ($wait in $stage.Waits) {
            $ok = $false
            if ($wait.Kind -eq 'Health') {
                Write-Step ("waiting for {0} healthcheck" -f $wait.Container)
                $ok = Wait-ContainerHealthy -Name $wait.Container -Timeout $TimeoutSec
            } else {
                Write-Step ("waiting for {0} readiness in logs" -f $wait.Container)
                $ok = Wait-ContainerLog -Name $wait.Container -Pattern $wait.Pattern -Timeout $TimeoutSec
            }
            if (-not $ok) {
                Stop-WithError ("{0} never became ready" -f $wait.Container) `
                    ("Logs above. The platform is left running so you can inspect it; run with -Down to clean up.")
            }
        }

        # The broker is the only hard ordering dependency, and topics are cheapest to create
        # immediately after it is healthy - before any consumer has subscribed.
        if ($stage.Repo -eq 'agentic-sdlc-eventbus') { New-PlatformTopics }
    }
}

function Show-Summary {
    Write-Banner 'PLATFORM READY'
    Show-Status
    Write-Host '  Endpoints' -ForegroundColor White
    Write-Host '    Tenant API          http://localhost:8000/health' -ForegroundColor Gray
    Write-Host '    MLflow UI           http://localhost:5000' -ForegroundColor Gray
    Write-Host '    Kafka (host)        localhost:9092' -ForegroundColor Gray
    Write-Host '    Kafka (containers)  host.docker.internal:9093' -ForegroundColor Gray
    Write-Host '    Control-plane DB    localhost:5433' -ForegroundColor Gray
    Write-Host ''
    Write-Host '  Follow the orchestrator:' -ForegroundColor White
    Write-Host '    docker logs -f agentic-sdlc-control-plane-consumer' -ForegroundColor Gray
    Write-Host ''
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

Write-Host ''
Write-Host '  agentic-sdlc platform launcher' -ForegroundColor White
Write-Host ("  root: {0}" -f $Root) -ForegroundColor DarkGray

if ($Down)   { Invoke-Down;  exit 0 }
if ($Status) { Show-Status;  exit 0 }

Test-Preflight
Start-Platform
Show-Summary

if ($Demo) {
    $demoScript = Join-Path $PSScriptRoot 'demo-end-to-end.ps1'
    if (Test-Path $demoScript) {
        & $demoScript -Root $Root
    } else {
        Write-Warn2 ("end-to-end demo script not found at {0}" -f $demoScript)
    }
}
