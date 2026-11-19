(function () {
    'use strict';

    // VT100-Ansicht: xterm.js wertet die Rohausgabe (Op output) aus, jede Eingabe geht direkt an die Shell
    const TYPE = __TYPE__;
    const RETRY_MS = 1000;

    const term = new Terminal({
        cursorBlink: true, scrollback: 5000, fontSize: 14,
        fontFamily: 'Menlo, Consolas, "DejaVu Sans Mono", monospace',
        theme: {background: '#1e1e1e'}
    });
    const fit = new FitAddon.FitAddon();
    term.loadAddon(fit);
    term.open(document.getElementById('vt100'));

    let pos = -1;
    let session = -1;
    let ended = false;

    async function job(data) {
        const body = new URLSearchParams();
        body.append('type', TYPE);
        for (const k in data) body.append(k, data[k]);
        const res = await fetch(location.protocol + '//' + location.host + '/', {method: 'POST', body: body});
        return res.json();
    }

    function notice(text, color) {
        term.write('\r\n\x1b[' + (color || 33) + 'm' + text + '\x1b[0m\r\n');
    }

    // Eingaben der Reihe nach senden, damit sich Tastendruecke nicht ueberholen
    let pending = '';
    let sending = false;

    async function flush() {
        if (sending || pending === '') return;
        sending = true;
        const text = pending;
        pending = '';
        try {
            const res = await job({op: 'input', text: text, enter: 0});
            if (!res.success) notice(res.message, 31);
        } catch (e) {
            notice(String(e), 31);
        }
        sending = false;
        flush();
    }

    term.onData(function (text) {
        pending += text;
        flush();
    });

    function apply(res) {
        if (res.reset) {
            term.reset();
            ended = false;
        }
        term.options.convertEol = res.mode === 'pipe';
        if (res.data) term.write(res.data);
        pos = res.pos;
        session = res.session;
        if (res.error) notice(res.error, 31);
        if (!res.running && !ended) {
            ended = true;
            const code = res.exit_code !== null && res.exit_code !== undefined ? ' (' + res.exit_code + ')' : '';
            notice('[Shell beendet' + code + ', eine Taste startet sie neu]');
        }
    }

    async function poll() {
        let delay = 0;
        try {
            const res = await job({op: 'output', pos: pos, session: session, wait: 2});
            if (res.success) apply(res); else delay = RETRY_MS;
            if (!res.running) delay = Math.max(delay, 500);
        } catch (e) {
            delay = RETRY_MS;
        }
        setTimeout(poll, delay);
    }

    // Groesse an das Fenster anpassen und dem Pseudo-Terminal mitteilen
    let resizeTimer = null;
    term.onResize(function (size) {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(function () {
            job({op: 'resize', cols: size.cols, rows: size.rows}).catch(function () {});
        }, 200);
    });
    new ResizeObserver(function () { fit.fit(); }).observe(document.getElementById('vt100'));

    fit.fit();
    job({op: 'resize', cols: term.cols, rows: term.rows}).catch(function () {});
    term.focus();
    poll();
})();
