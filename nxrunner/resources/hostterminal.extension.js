// Host Terminal: VS-Code-Terminal (Pseudoterminal), das die Shell des nxrunner-Terminal-Runners auf dem Host bedient.
// Die Verbindung laeuft vom Extension-Host im Container zu nxrunner (Einstellung nxrunner.hostApiUrl),
// nicht vom Browser aus, daher greift keine Sperre fuer Zugriffe auf das lokale Netzwerk.
const vscode = require('vscode');
const http = require('http');
const https = require('https');

const RETRY_MS = 2000;

function config() {
    const c = vscode.workspace.getConfiguration('nxrunner');
    return {url: c.get('hostApiUrl', ''), type: c.get('hostTerminalType', 'terminal')};
}

function sleep(ms) {
    return new Promise(function (resolve) { setTimeout(resolve, ms); });
}

// Job an nxrunner senden (POST wie terminal.js), Ergebnis als JSON
function job(data) {
    const cfg = config();
    if (!cfg.url) return Promise.reject(new Error('Einstellung nxrunner.hostApiUrl ist nicht gesetzt'));
    const body = new URLSearchParams(Object.assign({type: cfg.type}, data)).toString();
    const url = new URL(cfg.url.replace(/\/+$/, '') + '/');
    const lib = url.protocol === 'https:' ? https : http;
    return new Promise(function (resolve, reject) {
        const req = lib.request(url, {
            method: 'POST', timeout: 15000,
            headers: {'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body)}
        }, function (res) {
            let text = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) { text += chunk; });
            res.on('end', function () {
                try { resolve(JSON.parse(text)); } catch (e) { reject(new Error('Antwort ist kein JSON: ' + text.substring(0, 200))); }
            });
        });
        req.on('timeout', function () { req.destroy(new Error('Zeitueberschreitung')); });
        req.on('error', reject);
        req.end(body);
    });
}

class HostPty {
    constructor() {
        this.writeEmitter = new vscode.EventEmitter();
        this.onDidWrite = this.writeEmitter.event;
        this.pos = -1;
        this.session = -1;
        this.closed = false;
        this.ended = false;
        this.pending = '';
        this.sending = false;
        this.resizeTimer = null;
    }

    open(dimensions) {
        if (dimensions) this.setDimensions(dimensions);
        this.poll();
    }

    close() {
        this.closed = true;
    }

    notice(text, color) {
        this.writeEmitter.fire('\r\n\x1b[' + (color || 33) + 'm' + text + '\x1b[0m\r\n');
    }

    // Eingaben der Reihe nach senden, damit sich Tastendruecke nicht ueberholen
    handleInput(data) {
        this.pending += data;
        this.flush();
    }

    async flush() {
        if (this.sending || this.pending === '') return;
        this.sending = true;
        const text = this.pending;
        this.pending = '';
        try {
            const res = await job({op: 'input', text: text, enter: 0});
            if (!res.success) this.notice(res.message, 31);
        } catch (e) {
            this.notice(String(e.message || e), 31);
        }
        this.sending = false;
        this.flush();
    }

    setDimensions(dim) {
        clearTimeout(this.resizeTimer);
        this.resizeTimer = setTimeout(function () {
            job({op: 'resize', cols: dim.columns, rows: dim.rows}).catch(function () {});
        }, 200);
    }

    apply(res) {
        if (res.reset && this.session !== -1) {
            this.writeEmitter.fire('\x1bc');  // Terminal zuruecksetzen (Shell neu gestartet)
            this.ended = false;
        }
        let data = res.data || '';
        if (res.mode === 'pipe') data = data.replace(/\r?\n/g, '\r\n');
        if (data) this.writeEmitter.fire(data);
        this.pos = res.pos;
        this.session = res.session;
        if (res.error) this.notice(res.error, 31);
        if (!res.running && !this.ended) {
            this.ended = true;
            this.notice('[Shell beendet, eine Taste startet sie neu]');
        }
    }

    async poll() {
        let lastError = '';
        while (!this.closed) {
            try {
                const res = await job({op: 'output', pos: this.pos, session: this.session, wait: 2});
                lastError = '';
                if (!res.success) throw new Error(res.message || 'Fehler');
                this.apply(res);
                if (!res.running) await sleep(500);
            } catch (e) {
                const msg = 'Host Terminal: ' + (e.message || e) + ' (' + config().url + ')';
                if (msg !== lastError) this.notice(msg, 31);
                lastError = msg;
                await sleep(RETRY_MS);
            }
        }
    }
}

function createTerminal() {
    return vscode.window.createTerminal({name: 'Host', pty: new HostPty()});
}

function activate(context) {
    context.subscriptions.push(vscode.window.registerTerminalProfileProvider('nxrunner.hostTerminal', {
        provideTerminalProfile() {
            return new vscode.TerminalProfile({name: 'Host', pty: new HostPty()});
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('nxrunner.openHostTerminal', function () {
        createTerminal().show();
    }));
}

function deactivate() {
}

module.exports = {activate, deactivate};
