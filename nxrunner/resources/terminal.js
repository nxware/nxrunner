(function () {
    'use strict';

    const TYPE = __TYPE__;
    const STORE = 'nx_terminal_history_' + TYPE;
    const POLL_MS = 300;
    const $ = function (id) { return document.getElementById(id); };

    let version = '';
    let history = [];
    let historyPos = 0;

    async function job(data) {
        const body = new URLSearchParams();
        body.append('type', TYPE);
        for (const k in data) body.append(k, data[k]);
        const res = await fetch(location.protocol + '//' + location.host + '/', {method: 'POST', body: body});
        return res.json();
    }

    function render(res) {
        const screen = $('term_screen');
        screen.replaceChildren();
        res.lines.forEach(function (line, y) {
            const c = res.cursor;
            if (c && !c.hidden && c.y === y) {
                // Cursor als invertiertes Zeichen
                const x = Math.min(c.x, line.length - 1);
                const cur = document.createElement('span');
                cur.style.background = '#ddd';
                cur.style.color = '#111';
                cur.textContent = line.charAt(x) || ' ';
                screen.append(line.substring(0, x), cur, line.substring(x + 1));
            } else {
                screen.append(line);
            }
            if (y < res.lines.length - 1) screen.append('\n');
        });
        let status = (res.title ? res.title + ' – ' : '') + (res.mode || '');
        if (!res.running) status += ' – beendet' + (res.exit_code !== null && res.exit_code !== undefined ? ' (' + res.exit_code + ')' : '');
        $('term_status').textContent = status;
    }

    async function poll() {
        try {
            const res = await job({op: 'screen', version: version});
            if (res.success && !res.unchanged) {
                version = res.version;
                render(res);
            }
        } catch (e) {
            $('term_status').textContent = String(e);
        }
        setTimeout(poll, POLL_MS);
    }

    async function send(data) {
        const res = await job(data);
        if (!res.success) $('term_status').textContent = res.message;
    }

    function submit() {
        const text = $('term_input').value;
        $('term_input').value = '';
        if (text !== '' && history[history.length - 1] !== text) {
            history.push(text);
            history = history.slice(-100);
            try { localStorage.setItem(STORE, JSON.stringify(history)); } catch (e) {}
        }
        historyPos = history.length;
        send({op: 'input', text: text});
    }

    try { history = JSON.parse(localStorage.getItem(STORE)) || []; } catch (e) {}
    historyPos = history.length;

    $('term_send').onclick = submit;
    $('term_input').onkeydown = function (e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            submit();
        } else if (e.key === 'ArrowUp' && historyPos > 0) {
            e.preventDefault();
            $('term_input').value = history[--historyPos];
        } else if (e.key === 'ArrowDown') {
            e.preventDefault();
            historyPos = Math.min(historyPos + 1, history.length);
            $('term_input').value = history[historyPos] || '';
        } else if (e.key === 'c' && e.ctrlKey && $('term_input').selectionStart === $('term_input').selectionEnd) {
            e.preventDefault();
            send({op: 'key', key: 'ctrl_c'});
        } else if (e.key === 'Tab') {
            // Text ohne Enter senden, damit die Shell vervollstaendigen kann
            e.preventDefault();
            send({op: 'input', text: $('term_input').value + '\t', enter: 0});
            $('term_input').value = '';
        }
    };
    document.querySelectorAll('.term_key').forEach(function (btn) {
        btn.onclick = function () { send({op: 'key', key: btn.dataset.key}); $('term_input').focus(); };
    });
    $('term_restart').onclick = function () { send({op: 'restart'}); };
    $('term_input').focus();
    poll();
})();
