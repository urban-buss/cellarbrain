/**
 * dashboard.js — Chart.js render helpers for the observability dashboard.
 * Loaded in base.html, called from inline <script> blocks in templates.
 */

function renderBarChart(canvasId, apiUrl, label) {
    fetch(apiUrl)
        .then(r => r.json())
        .then(d => {
            new Chart(document.getElementById(canvasId), {
                type: 'bar',
                data: {
                    labels: d.labels,
                    datasets: [{
                        label: label || 'Calls',
                        data: d.data,
                        backgroundColor: 'rgba(99, 102, 241, 0.5)',
                        borderColor: 'rgb(99, 102, 241)',
                        borderWidth: 1,
                    }]
                },
                options: { responsive: true, scales: { y: { beginAtZero: true } } }
            });
        });
}

function renderLatencyChart(canvasId, apiUrl) {
    fetch(apiUrl)
        .then(r => r.json())
        .then(d => {
            new Chart(document.getElementById(canvasId), {
                type: 'line',
                data: {
                    labels: d.labels,
                    datasets: [
                        { label: 'P50', data: d.p50, borderColor: '#22c55e', fill: false },
                        { label: 'P95', data: d.p95, borderColor: '#ef4444', fill: false },
                    ]
                },
                options: { responsive: true, scales: { y: { beginAtZero: true } } }
            });
        });
}

function renderHistogram(canvasId, apiUrl) {
    fetch(apiUrl)
        .then(r => r.json())
        .then(d => {
            new Chart(document.getElementById(canvasId), {
                type: 'bar',
                data: {
                    labels: d.buckets,
                    datasets: [{
                        label: 'Count',
                        data: d.counts,
                        backgroundColor: 'rgba(234, 179, 8, 0.5)',
                    }]
                },
                options: { responsive: true, scales: { y: { beginAtZero: true } } }
            });
        });
}

function refreshConnection() {
    fetch('/api/refresh', { method: 'POST' })
        .then(r => r.json())
        .then(() => window.location.reload());
}

function renderDoughnutChart(canvasId, labels, data) {
    const colors = [
        '#6366f1', '#ec4899', '#f59e0b', '#10b981',
        '#3b82f6', '#8b5cf6', '#ef4444', '#14b8a6', '#94a3b8'
    ];
    new Chart(document.getElementById(canvasId), {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: colors.slice(0, data.length),
            }]
        },
        options: { responsive: true, plugins: { legend: { position: 'right' } } }
    });
}

function renderLineChart(canvasId, datasets) {
    new Chart(document.getElementById(canvasId), {
        type: 'line',
        data: { datasets: datasets },
        options: {
            responsive: true,
            scales: {
                x: { type: 'category', title: { display: true, text: 'Month' } },
                y: { beginAtZero: false, title: { display: true, text: 'Price (CHF)' } },
            },
            plugins: { legend: { position: 'top' } },
        }
    });
}

function renderVelocityChart(canvasId, labels, acquired, consumed) {
    new Chart(document.getElementById(canvasId), {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Acquired',
                    data: acquired,
                    backgroundColor: 'rgba(16, 185, 129, 0.6)',
                    borderColor: '#10b981',
                    borderWidth: 1,
                },
                {
                    label: 'Consumed',
                    data: consumed,
                    backgroundColor: 'rgba(99, 102, 241, 0.6)',
                    borderColor: '#6366f1',
                    borderWidth: 1,
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { grid: { display: false } },
                y: { beginAtZero: true, ticks: { precision: 0 } }
            },
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, font: { size: 11 } } }
            }
        }
    });
}
