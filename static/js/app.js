$(function () {
  // ── Sidebar toggle ──
  $('#sidebar-toggle').on('click', function () {
    $('body').toggleClass('sidebar-collapsed');
    localStorage.setItem('sidebar-collapsed', $('body').hasClass('sidebar-collapsed'));
  });
  if (localStorage.getItem('sidebar-collapsed') === 'true') {
    $('body').addClass('sidebar-collapsed');
  }

  // ── Sidebar submenu toggle ──
  $('.has-sub > a').on('click', function (e) {
    e.preventDefault();
    if ($('body').hasClass('sidebar-collapsed')) return;
    $(this).parent().toggleClass('open');
  });

  // ── Mobile sidebar ──
  $('#mobile-toggle').on('click', function () {
    $('#sidebar').toggleClass('mobile-open');
  });
  $(document).on('click', function (e) {
    if ($(window).width() <= 768 && !$(e.target).closest('#sidebar, #mobile-toggle').length) {
      $('#sidebar').removeClass('mobile-open');
    }
  });

  // ── Tabs ──
  $(document).on('click', '.tab-item', function () {
    var target = $(this).data('tab');
    $(this).siblings().removeClass('active');
    $(this).addClass('active');
    $(this).closest('.card, .page-content, .tab-wrapper').find('.tab-content').removeClass('active');
    $('#' + target).addClass('active');
  });

  // ── Chart.js default config ──
  if (typeof Chart !== 'undefined') {
    Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
    Chart.defaults.font.size = 12;
    Chart.defaults.color = '#64748b';
    Chart.defaults.plugins.legend.labels.usePointStyle = true;
    Chart.defaults.plugins.legend.labels.padding = 16;
    Chart.defaults.elements.line.tension = 0.3;
    Chart.defaults.elements.point.radius = 2;
    Chart.defaults.elements.point.hoverRadius = 5;
  }
});

// ── Modal helpers ──
function openModal(title, bodyHtml, footerHtml, cls) {
  $('#modal-title').text(title);
  $('#modal-body').html(bodyHtml);
  $('#modal-footer').html(footerHtml || '<button class="btn btn-outline" onclick="closeModal()">' + __t('common.close') + '</button>');
  $('#modal-container').attr('class', 'modal ' + (cls || ''));
  $('#modal-overlay').removeClass('hidden');
}
function closeModal() { $('#modal-overlay').addClass('hidden'); }

// 모달 외부 클릭으로 닫기 (모든 .modal-overlay 공통).
//
// 브라우저는 mousedown 과 mouseup 의 target 이 다르면 둘의 공통 조상에서
// click 이벤트를 발생시킨다. 사용자가 모달 안의 input 에서 좌클릭으로 텍스트
// 드래그를 시작해 외부 (오버레이) 까지 끌고 가서 놓으면 click.target=오버레이 가
// 되어 의도치 않게 모달이 닫힌다. 이 페이지에는 인라인 onclick="if(event.target
// ===this)closeXxx()" 패턴도 여러 군데 있으므로 capture phase 에서 한 번에 차단.
(function () {
  document.addEventListener('mousedown', function (e) {
    var ovr = e.target.closest && e.target.closest('.modal-overlay');
    if (ovr) ovr.__mdInside = (e.target !== ovr);
  }, true);
  document.addEventListener('click', function (e) {
    var ovr = e.target.closest && e.target.closest('.modal-overlay');
    // 차단 조건: click 이벤트의 target 이 overlay 자체 (드래그-아웃의 공통 조상
    // 마커) 이면서 mousedown 은 내부에서 시작된 경우. 모달 내부 버튼/입력 클릭은
    // target !== ovr 이므로 통과시켜 X 버튼 등 정상 동작 유지.
    if (ovr && e.target === ovr && ovr.__mdInside) {
      e.stopImmediatePropagation();
      e.stopPropagation();
    }
    if (ovr) ovr.__mdInside = false;
  }, true);
})();

// 기본 #modal-overlay 외부 클릭 = 닫기 (위 capture 가드 통과한 경우만).
$(document).on('click', '#modal-overlay', function (e) {
  if (e.target === this) closeModal();
});

// ── Confirm dialog ──
function confirmAction(msg, callback) {
  openModal(__t('common.confirm'), '<p>' + msg + '</p>',
    '<button class="btn btn-outline" onclick="closeModal()">' + __t('common.cancel') + '</button>' +
    '<button class="btn btn-primary" id="confirm-ok">' + __t('common.confirm') + '</button>'
  );
  $('#confirm-ok').on('click', function () { closeModal(); if (callback) callback(); });
}

// ── Manual-refresh helper: 사용자 trigger 새로고침 후 모든 AJAX 가 끝나면 토스트 표시.
// 인자는 jqXHR 를 return 하는 loader 함수들. (자동 polling 등 다른 호출자는 사용 ✗)
function _refreshWithToast() {
  var jobs = Array.prototype.slice.call(arguments)
    .map(function (fn) { return typeof fn === 'function' ? fn() : null; })
    .filter(function (j) { return j && typeof j.always === 'function'; });
  var done = function (ok) {
    var msg = (typeof __t === 'function') ? __t('common.refresh_done') : 'Refreshed';
    showToast(msg, ok ? 'success' : 'warning');
  };
  if (!jobs.length) { done(true); return; }
  $.when.apply($, jobs).done(function () { done(true); }).fail(function () { done(false); });
}

// ── Toast notification ──
function showToast(msg, type) {
  type = type || 'info';
  var colors = { info: '#3b82f6', success: '#22c55e', warning: '#f59e0b', error: '#ef4444' };
  var icons = { info: 'circle-info', success: 'circle-check', warning: 'triangle-exclamation', error: 'circle-xmark' };
  var $t = $('<div>').css({
    position: 'fixed', top: '72px', right: '24px', zIndex: 3000,
    background: '#fff', borderLeft: '4px solid ' + colors[type], borderRadius: '8px',
    padding: '12px 20px', boxShadow: '0 4px 12px rgba(0,0,0,.12)',
    display: 'flex', alignItems: 'center', gap: '10px', fontSize: '13px',
    animation: 'slideIn .3s ease'
  }).html('<i class="fas fa-' + icons[type] + '" style="color:' + colors[type] + '"></i>' + msg).appendTo('body');
  setTimeout(function () { $t.fadeOut(300, function () { $t.remove(); }); }, 3000);
}

// ── Simple chart creators ──
function createLineChart(canvasId, labels, datasets, opts) {
  var ctx = document.getElementById(canvasId);
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'line',
    data: { labels: labels, datasets: datasets },
    options: $.extend(true, {
      responsive: true, maintainAspectRatio: false,
      scales: { y: { beginAtZero: true, grid: { color: '#f1f5f9' } }, x: { grid: { display: false } } },
      plugins: { legend: { position: 'top' } }
    }, opts || {})
  });
}

function createBarChart(canvasId, labels, datasets, opts) {
  var ctx = document.getElementById(canvasId);
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'bar',
    data: { labels: labels, datasets: datasets },
    options: $.extend(true, {
      responsive: true, maintainAspectRatio: false,
      scales: { y: { beginAtZero: true, grid: { color: '#f1f5f9' } }, x: { grid: { display: false } } },
      plugins: { legend: { position: 'top' } }
    }, opts || {})
  });
}

function createDoughnutChart(canvasId, labels, data, colors) {
  var ctx = document.getElementById(canvasId);
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'doughnut',
    data: { labels: labels, datasets: [{ data: data, backgroundColor: colors, borderWidth: 0 }] },
    options: { responsive: true, maintainAspectRatio: false, cutout: '65%', plugins: { legend: { position: 'bottom' } } }
  });
}

// ── Format helpers ──
function fmtNum(n) { return n.toLocaleString(); }
function fmtPct(n) { return n.toFixed(1) + '%'; }
function fmtBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  if (b < 1073741824) return (b / 1048576).toFixed(1) + ' MB';
  return (b / 1073741824).toFixed(1) + ' GB';
}

// ── Slideout animation ──
var styleEl = document.createElement('style');
styleEl.textContent = '@keyframes slideIn{from{transform:translateX(100px);opacity:0}to{transform:translateX(0);opacity:1}}';
document.head.appendChild(styleEl);
