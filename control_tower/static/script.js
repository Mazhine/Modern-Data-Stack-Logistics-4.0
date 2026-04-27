// --- Auth Logic (Relying on Cookies) ---
const logoutBtn = document.getElementById('logout-btn');
if (logoutBtn) {
    logoutBtn.addEventListener('click', () => {
        window.location.href = '/logout';
    });
}

// --- DOM Elements ---
const navItems = document.querySelectorAll('.nav-item');
const mainFrame = document.getElementById('main-frame');
const externalLink = document.getElementById('external-link');
const btnStreamStart = document.getElementById('btn-stream-start');
const btnStreamStop = document.getElementById('btn-stream-stop');
const btnThemeToggle = document.getElementById('btn-theme-toggle');

// --- Theme Toggle ---
if (btnThemeToggle) {
    btnThemeToggle.addEventListener('click', () => {
        const isDark = document.body.classList.contains('dark-theme');
        if (isDark) {
            document.body.classList.remove('dark-theme');
            document.body.classList.add('light-theme');
            btnThemeToggle.innerHTML = '<i data-lucide="moon"></i> Dark';
        } else {
            document.body.classList.remove('light-theme');
            document.body.classList.add('dark-theme');
            btnThemeToggle.innerHTML = '<i data-lucide="sun"></i> Light';
        }
        lucide.createIcons();
    });
}

const streamModeSelect = document.getElementById('stream-mode');
const containerList = document.getElementById('container-list');
const statMode = document.getElementById('stat-mode');

// --- Navigation ---
navItems.forEach(item => {
    item.addEventListener('click', () => {
        const target = item.getAttribute('data-target');
        const url = item.getAttribute('data-url');
        
        // Update Nav
        navItems.forEach(i => i.classList.remove('active'));
        item.classList.add('active');
        
        // Cacher toutes les vues
        const views = ['view-dashboard', 'view-iframe', 'view-postgres'];
        views.forEach(v => {
            const el = document.getElementById(v);
            if (el) el.classList.add('hidden');
        });

        // Switch Views
        if (target === 'dashboard') {
            const el = document.getElementById('view-dashboard');
            if (el) el.classList.remove('hidden');
        } else if (target === 'postgres') {
            const el = document.getElementById('view-postgres');
            if (el) el.classList.remove('hidden');
            loadDbTables();
        } else {
            const el = document.getElementById('view-iframe');
            if (el) el.classList.remove('hidden');
            if (mainFrame) mainFrame.src = url;
            if (externalLink) externalLink.href = url;
            showCredentialBanner(target);
        }
    });
});

// --- Credential Tooltip ---
const SERVICE_CREDS = {
    'airflow': { service: 'Airflow',  user: 'admin',   pass: 'admin'    },
    'minio':   { service: 'MinIO',    user: 'admin',   pass: 'password' },
    'grafana': { service: 'Grafana',  user: 'admin',   pass: 'admin'    },
};

function showCredentialBanner(target) {
    const tooltip  = document.getElementById('credential-tooltip');
    const toggle   = document.getElementById('cred-toggle');
    const creds    = SERVICE_CREDS[target];
    if (!creds || !tooltip || !toggle) { 
        if(tooltip) tooltip.classList.add('hidden'); 
        if(toggle) toggle.classList.add('hidden'); 
        return; 
    }
    document.getElementById('cred-service-label').textContent = '🔑 ' + creds.service;
    document.getElementById('cred-u').textContent = creds.user;
    document.getElementById('cred-p').textContent = creds.pass;
    toggle.classList.remove('hidden');
    tooltip.classList.remove('hidden');
    clearTimeout(tooltip._t);
    tooltip._t = setTimeout(() => tooltip.classList.add('hidden'), 6000);
    if (window.lucide) lucide.createIcons();
}

function toggleCredTooltip() {
    const t = document.getElementById('credential-tooltip');
    if (t) t.classList.toggle('hidden');
    if (window.lucide) lucide.createIcons();
}

function quickCopy(id, btn) {
    const el = document.getElementById(id);
    if (!el) return;
    navigator.clipboard.writeText(el.textContent).then(() => {
        const orig = btn.innerHTML;
        btn.innerHTML = '✓';
        setTimeout(() => { btn.innerHTML = orig; if (window.lucide) lucide.createIcons(); }, 1500);
    });
}

// --- Architecture Node Clicks ---
const nodeMap = {
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

Object.entries(nodeMap).forEach(([nodeId, target]) => {
    const nodeEl = document.getElementById(nodeId);
    if (nodeEl) {
        nodeEl.style.cursor = 'pointer';
        nodeEl.addEventListener('click', () => {
            const navItem = document.querySelector(`.nav-item[data-target="${target}"]`);
            if (navItem) navItem.click();
        });
    }
});

// --- Stream Control ---
if (btnStreamStart) {
    btnStreamStart.addEventListener('click', async () => {
        const mode = streamModeSelect.value;
        const delay = document.getElementById('stream-delay').value;
        await fetch(`/stream/start?mode=${mode}&delay=${delay}`, { method: 'POST' });
        updateStatus();
    });
}

if (btnStreamStop) {
    btnStreamStop.addEventListener('click', async () => {
        await fetch('/stream/stop', { method: 'POST' });
        updateStatus();
        addTerminalLog("Flux interrompu par l'utilisateur.", 'SYS', 'term-sys');
    });
}

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
    if (terminalWindow.childElementCount > 30) terminalWindow.removeChild(terminalWindow.firstChild);
    terminalWindow.scrollTop = terminalWindow.scrollHeight;
}

function startTerminalSimulator() {
    if (termInterval) clearInterval(termInterval);
    termInterval = setInterval(() => {
        if (btnStreamStart && !btnStreamStart.classList.contains('hidden')) return;
        if (!statMode) return;
        const mode = statMode.innerText;
        let msg = logMessages[Math.floor(Math.random() * logMessages.length)];
        addTerminalLog(msg, 'APP', 'term-msg');
    }, 1500);
}

// --- Polling & Status ---
async function updateStatus() {
    try {
        const res = await fetch('/status');
        if (res.status === 401) { window.location.href = "/"; return; }
        const data = await res.json();
        
        if (containerList && data.containers) {
            containerList.innerHTML = '';
            data.containers.sort((a,b) => a.name.localeCompare(b.name)).forEach(c => {
                const item = document.createElement('div');
                item.className = 'container-item';
                const statusClass = c.status === 'running' ? 'status-running' : 'status-stopped';
                item.innerHTML = `<span>${c.name}</span><span class="${statusClass}">${c.status.toUpperCase()}</span>`;
                containerList.appendChild(item);
            });
        }
        
        if (btnStreamStart && btnStreamStop) {
            if (data.stream_active) {
                btnStreamStart.classList.add('hidden');
                btnStreamStop.classList.remove('hidden');
                if (statMode) statMode.innerText = data.stream_mode.toUpperCase();
            } else {
                btnStreamStart.classList.remove('hidden');
                btnStreamStop.classList.add('hidden');
                if (statMode) statMode.innerText = 'IDLE';
            }
        }
    } catch (err) {
        console.error("Failed to poll status", err);
    }
}

function startPolling() {
    updateStatus();
    startTerminalSimulator();
    setInterval(updateStatus, 2000);
}

document.addEventListener('DOMContentLoaded', startPolling);

// --- DB Viewer ---
async function loadDbTables() {
    const listEl = document.getElementById('db-table-list');
    if (!listEl) return;
    try {
        const res = await fetch('/db/tables');
        const data = await res.json();
        listEl.innerHTML = '';
        data.tables.forEach(t => {
            const btn = document.createElement('button');
            btn.className = 'db-table-btn';
            btn.innerHTML = `<span class="db-table-name">${t.name}</span><span class="db-table-meta">${t.rows} lignes</span>`;
            btn.onclick = () => loadDbTable(t.name);
            listEl.appendChild(btn);
        });
    } catch (e) {}
}

async function loadDbTable(tableName) {
    const dataEl = document.getElementById('db-table-data');
    if (!dataEl) return;
    try {
        const res = await fetch(`/db/table/${tableName}?limit=50`);
        const data = await res.json();
        let html = '<table class="db-table"><thead><tr>';
        data.columns.forEach(c => { html += `<th>${c}</th>`; });
        html += '</tr></thead><tbody>';
        data.rows.forEach(row => {
            html += '<tr>';
            data.columns.forEach(c => { html += `<td>${row[c]}</td>`; });
            html += '</tr>';
        });
        html += '</tbody></table>';
        dataEl.innerHTML = html;
    } catch (e) {}
}
