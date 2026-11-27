"""
Gemeinsames JavaScript fuer Fortschrittsanzeigen langer Jobs (op=status wird zyklisch abgefragt).
"""

# nx_poll_status(type, el_id, render, reload_ms):
#   fragt jede Sekunde {type, op: 'status'} ab und ruft render(element, status) auf, solange das Element existiert.
#   Ist status.running false, endet die Abfrage und die Seite wird nach reload_ms neu geladen.
#   Pro el_id laeuft hoechstens eine Abfrage.
POLL_STATUS_JS = """
function nx_poll_status(type, el_id, render, reload_ms) {
  var timers = window.nx_poll_timers = window.nx_poll_timers || {};
  if (timers[el_id]) return;
  var stop = function() { clearInterval(timers[el_id]); timers[el_id] = null; };
  var poll = function() {
    exec_job({type: type, op: 'status'}, function(s) {
      var $p = document.getElementById(el_id);
      if ($p === null) { stop(); return; }
      render($p, s);
      if (!s.running) {
        stop();
        setTimeout(function() { location.reload(); }, reload_ms);
      }
    });
  };
  timers[el_id] = setInterval(poll, 1000);
  poll();
}
"""
