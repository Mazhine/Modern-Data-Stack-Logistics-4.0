let currentToken = "";
let pollInterval = null;

// --- DOM Elements ---
const loginScreen = document.getElementById('login-screen');
const mainApp = document.getElementById('main-app');
const loginBtn = document.getElementById('login-btn');
const usernameInput = document.getElementById('username');
const passwordInput = document.getElementById('password');
const loginError = document.getElementById('login-error');

const navItems = document.querySelectorAll('.nav-item');
const subViews = document.querySelectorAll('.sub-view');
const mainFrame = document.getElementById('main-frame');
const iframeBlocked = document.getElementById('iframe-blocked');
const externalLink = document.getElementById('external-link');

const btnStreamStart = document.getElementById('btn-stream-start');
const btnStreamStop = document.getElementById('btn-stream-stop');
const streamModeSelect = document.getElementById('stream-mode');
const containerList = document.getElementById('container-list');
const statMode = document.getElementById('stat-mode');

// --- Auth Logic ---
loginBtn.addEventListener('click', async () => {
    const user = usernameInput.value;
    const pass = passwordInput.value;
    
    // Create Basic Auth header
    const token = btoa(`${user}:${pass}`);
    
    try {
        const res = await fetch('/api/status', {
            headers: { 'Authorization': `Basic ${token}` }
        });
        
        if (res.ok) {
            currentToken = token;
            loginScreen.classList.add('hidden');
            mainApp.classList.remove('hidden');
            document.getElementById('display-user').innerText = user;
            startPolling();
        } else {
            loginError.innerText = "Identifiants invalides ou serveur indisponible.";
        }
    } catch (err) {
        loginError.innerText = "Erreur de connexion au Control Tower.";
    }
});

document.getElementById('logout-btn').addEventListener('click', () => {
    currentToken = "";
    clearInterval(pollInterval);
    window.location.reload();
});

// --- Navigation ---
navItems.forEach(item => {
    item.addEventListener('click', () => {
        const target = item.getAttribute('data-target');
        const url = item.getAttribute('data-url');
        
        // Update Nav
        navItems.forEach(i => i.classList.remove('active'));
        item.classList.add('active');
        
        // Toujours cacher toutes les vues d'abord
        document.getElementById('view-dashboard').classList.add('hidden');
        document.getElementById('view-iframe').classList.add('hidden');
        document.getElementById('view-postgres').classList.add('hidden');

        // Switch Views
        if (target === 'dashboard') {
            document.getElementById('view-dashboard').classList.remove('hidden');
        } else if (target === 'postgres') {
            document.getElementById('view-postgres').classList.remove('hidden');
            loadDbTables();
        } else {
            document.getElementById('view-iframe').classList.remove('hidden');
            mainFrame.src = url;
            externalLink.href = url;
        }
    });
});

// --- Architecture Node Clicks ---
const nodes = {
    'node-source': 'dashboard',
    'node-kafka': 'kafka',
    'node-spark': 'spark',
    'node-minio': 'minio',
    'node-spark-process': 'spark',
    'node-postgres': 'postgres',
    'node-airflow': 'airflow',
    'node-grafana': 'grafana',
    'node-mlflow': 'mlflow'
};

Object.entries(nodes).forEach(([nodeId, target]) => {
    const nodeEl = document.getElementById(nodeId);
    if (nodeEl) {
        nodeEl.style.cursor = 'pointer';
        nodeEl.addEventListener('click', () => {
            console.log(`Node clicked: ${nodeId} -> target: ${target}`);
            const navItem = document.querySelector(`.nav-item[data-target="${target}"]`);
            if (navItem) navItem.click();
        });
    }
});

// Detect Iframe Loading
mainFrame.onload = function() {
    console.log("Iframe loaded: " + mainFrame.src);
    // We no longer block with an overlay, as it false-triggers on cross-origin
};

// --- Stream Control ---
btnStreamStart.addEventListener('click', async () => {
    const mode = streamModeSelect.value;
    const delay = document.getElementById('stream-delay').value;
    await fetch(`/api/stream/start?mode=${mode}&delay=${delay}`, {
        method: 'POST',
        headers: { 'Authorization': `Basic ${currentToken}` }
    });
    updateStatus();
});

btnStreamStop.addEventListener('click', async () => {
    await fetch('/api/stream/stop', {
        method: 'POST',
        headers: { 'Authorization': `Basic ${currentToken}` }
    });
    updateStatus();
    addTerminalLog("Flux interrompu par l'utilisateur.", 'SYS', 'term-sys');
});

// --- Terminal Simulator ---
const terminalWindow = document.getElementById('terminal-window');
let termInterval = null;
const logMessages = [
    "[KAFKA] Batch stream committed: 500 events",
    "[SPARK] Processed micro-batch successfully",
    "[SPARK] Writing 500 safe records to Data Lake",
    "[DB] PostgreSQL sink committed transaction",
    "[LOKI] Telemetry flushed successfully",
    "[SYS] Resources monitoring: OK"
];

function addTerminalLog(msg, prefix = "APP", cssClass = "term-msg") {
    if (!terminalWindow) return;
    const div = document.createElement('div');
    div.className = 'term-line';
    const time = new Date().toISOString().split('T')[1].slice(0,8);
    div.innerHTML = `<span class="term-time">[${time}]</span> <span class="${cssClass}">[${prefix}] ${msg}</span>`;
    terminalWindow.appendChild(div);
    if (terminalWindow.childElementCount > 30) {
        terminalWindow.removeChild(terminalWindow.firstChild);
    }
    terminalWindow.scrollTop = terminalWindow.scrollHeight;
}

function startTerminalSimulator() {
    if (termInterval) clearInterval(termInterval);
    termInterval = setInterval(() => {
        if (!document.getElementById('btn-stream-start').classList.contains('hidden')) return;

        const mode = statMode.innerText;
        let msg = logMessages[Math.floor(Math.random() * logMessages.length)];

        if (mode.includes('CHAOS') && Math.random() > 0.7) {
            msg = '[ALERT] Chaos Injection: Data corruption detected in pipeline!';
            addTerminalLog(msg, 'ERR', 'term-sys');
        } else if (mode.includes('AIO') && Math.random() > 0.6) {
            const aioMsgs = [
                '[AIO:BOUNDARY] Uncertainty sampling → edge case redirected',
                '[AIO:NOMINAL] Real dataset event dispatched (35% baseline)',
                '[AIO:MIX] SmartMix anomaly injected: SCHEDULING_PARADOX',
                '[AIO:CHAOS] Extreme case → revenue integrity violation',
                '[AIO:STATS] dist={nominal:35%, boundary:31%, mix:20%, chaos:14%}',
            ];
            msg = aioMsgs[Math.floor(Math.random() * aioMsgs.length)];
            addTerminalLog(msg, 'AIO', 'term-highlight');
        } else if (mode.includes('IA') && Math.random() > 0.6) {
            const iaMsgs = [
                '[AI:BOUNDARY] error_rate=0.287 — injecting edge case',
                '[AI:NOMINAL] Regime CHALLENGING — model doing well',
                '[AI:RESET] High error detected — sending clean data',
                '[AI:STATS] {"error_rate":0.31,"regime":"BOUNDARY"}',
            ];
            msg = iaMsgs[Math.floor(Math.random() * iaMsgs.length)];
            addTerminalLog(msg, 'AI', 'term-success');
        } else {
            addTerminalLog(msg, 'APP', 'term-msg');
        }
    }, 1500);
}

// --- Polling & Status ---
async function updateStatus() {
    if (!currentToken) return;
    
    try {
        const res = await fetch('/api/status', {
            headers: { 'Authorization': `Basic ${currentToken}` }
        });
        const data = await res.json();
        
        // Update Containers
        containerList.innerHTML = '';
        if (data.containers) {
            data.containers.sort((a,b) => a.name.localeCompare(b.name)).forEach(c => {
                const item = document.createElement('div');
                item.className = 'container-item';
                const statusClass = c.status === 'running' ? 'status-running' : 'status-stopped';
                item.innerHTML = `
                    <span>${c.name}</span>
                    <span class="${statusClass}">${c.status.toUpperCase()}</span>
                `;
                containerList.appendChild(item);
            });
        }
        
        // Update Stream Buttons
        if (data.stream_active) {
            btnStreamStart.classList.add('hidden');
            btnStreamStop.classList.remove('hidden');
            
            // Update Medallion Streaming Animations
            const flowchart = document.querySelector('.flowchart-container');
            if (flowchart) {
                flowchart.classList.add('streaming-active');
            }
            
            statMode.innerText = data.stream_mode.toUpperCase();
            // Couleur selon le mode
            const modeColors = {
                'sain':  'var(--accent-emerald)',
                'mix':   'var(--accent-cyan)',
                'chaos': 'var(--accent-red)',
                'ia':    '#a78bfa',   // violet
                'aio':   '#f59e0b'    // doré premium
            };
            statMode.style.color = modeColors[data.stream_mode] || 'var(--accent-blue)';
            // Start arrows animation
            document.querySelectorAll('.arrow, .arrow-vertical').forEach(a => a.classList.add('passing'));
        } else {
            btnStreamStart.classList.remove('hidden');
            btnStreamStop.classList.add('hidden');
            
            // Update Medallion Streaming Animations
            const flowchart = document.querySelector('.flowchart-container');
            if (flowchart) {
                flowchart.classList.remove('streaming-active');
            }
            
            statMode.innerText = 'IDLE';
            statMode.style.color = 'var(--text-secondary)';
            // Stop arrows animation
            document.querySelectorAll('.arrow, .arrow-vertical').forEach(a => a.classList.remove('passing'));
        }
        
    } catch (err) {
        console.error("Failed to poll status", err);
    }
}

function startPolling() {
    if (pollInterval) clearInterval(pollInterval);
    updateStatus();
    startTerminalSimulator();
    pollInterval = setInterval(updateStatus, 2000);
}

startPolling();

// ==============================================================================
// POSTGRESQL NATIVE VIEWER
// ==============================================================================
async function loadDbTables() {
    const listEl = document.getElementById('db-table-list');
    const headerEl = document.getElementById('db-table-header');
    const dataEl = document.getElementById('db-table-data');
    listEl.innerHTML = '<div style="color:var(--text-secondary);padding:10px;">Chargement...</div>';
    dataEl.innerHTML = '';
    try {
        const res = await fetch('/db/tables', { headers: { 'Authorization': `Basic ${currentToken}` } });
        const data = await res.json();
        if (data.error) {
            listEl.innerHTML = `<div style="color:#ef4444;padding:10px;">Erreur: ${data.error}</div>`;
            return;
        }
        listEl.innerHTML = '';
        if (!data.tables || data.tables.length === 0) {
            listEl.innerHTML = '<div style="color:var(--text-secondary);padding:10px;font-size:0.8rem;">Aucune table trouvée.<br>Lancez le stream pour peupler la DB.</div>';
            return;
        }
        data.tables.forEach(t => {
            const btn = document.createElement('button');
            btn.className = 'db-table-btn';
            btn.innerHTML = `<span class="db-table-name">${t.name}</span><span class="db-table-meta">${t.rows} lignes · ${t.size}</span>`;
            btn.onclick = () => {
                document.querySelectorAll('.db-table-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                loadDbTable(t.name);
            };
            listEl.appendChild(btn);
        });
        headerEl.textContent = `${data.tables.length} table(s) dans logistics_db`;
    } catch (e) {
        listEl.innerHTML = `<div style="color:#ef4444;padding:10px;">Connexion PostgreSQL échouée</div>`;
    }
}

async function loadDbTable(tableName) {
    const dataEl = document.getElementById('db-table-data');
    const headerEl = document.getElementById('db-table-header');
    dataEl.innerHTML = '<div style="color:var(--text-secondary);padding:10px;">Chargement des données...</div>';
    try {
        const res = await fetch(`/db/table/${tableName}?limit=100`, { headers: { 'Authorization': `Basic ${currentToken}` } });
        const data = await res.json();
        if (data.error) {
            dataEl.innerHTML = `<div style="color:#ef4444;">Erreur: ${data.error}</div>`;
            return;
        }
        headerEl.textContent = `Table: ${tableName} — ${data.rows.length} dernières lignes`;
        if (!data.rows.length) {
            dataEl.innerHTML = '<div style="color:var(--text-secondary);padding:20px;text-align:center;">Table vide — lancez le stream pour insérer des données.</div>';
            return;
        }
        let html = '<table class="db-table"><thead><tr>';
        data.columns.forEach(c => { html += `<th>${c}</th>`; });
        html += '</tr></thead><tbody>';
        data.rows.forEach(row => {
            html += '<tr>';
            data.columns.forEach(c => {
                const val = row[c] ?? '';
                html += `<td>${String(val).substring(0, 60)}</td>`;
            });
            html += '</tr>';
        });
        html += '</tbody></table>';
        dataEl.innerHTML = html;
    } catch (e) {
        dataEl.innerHTML = `<div style="color:#ef4444;">Erreur réseau</div>`;
    }
}
