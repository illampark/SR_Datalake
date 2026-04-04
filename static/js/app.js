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
