<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Crime Count Forecasting — Apache Spark</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@300;400;500&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0a0a0f;
    --surface: #111118;
    --surface2: #1a1a24;
    --border: #2a2a3a;
    --accent: #e85d4a;
    --accent2: #4a9eff;
    --accent3: #a78bfa;
    --text: #e8e8f0;
    --muted: #6b6b85;
    --green: #34d399;
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'DM Mono', monospace;
    line-height: 1.7;
    min-height: 100vh;
  }

  /* Grid background */
  body::before {
    content: '';
    position: fixed;
    inset: 0;
    background-image: 
      linear-gradient(rgba(74,158,255,0.03) 1px, transparent 1px),
      linear-gradient(90deg, rgba(74,158,255,0.03) 1px, transparent 1px);
    background-size: 40px 40px;
    pointer-events: none;
    z-index: 0;
  }

  .container {
    max-width: 860px;
    margin: 0 auto;
    padding: 60px 32px;
    position: relative;
    z-index: 1;
  }

  /* Header */
  .header {
    margin-bottom: 64px;
    animation: fadeUp 0.6s ease both;
  }

  .badge {
    display: inline-block;
    background: rgba(232,93,74,0.12);
    border: 1px solid rgba(232,93,74,0.3);
    color: var(--accent);
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    padding: 5px 14px;
    border-radius: 4px;
    margin-bottom: 20px;
  }

  h1 {
    font-family: 'Syne', sans-serif;
    font-size: clamp(32px, 5vw, 52px);
    font-weight: 800;
    line-height: 1.1;
    letter-spacing: -0.02em;
    margin-bottom: 16px;
  }

  h1 span {
    color: var(--accent);
  }

  .subtitle {
    color: var(--muted);
    font-size: 14px;
    max-width: 560px;
    line-height: 1.8;
  }

  .tech-stack {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 24px;
  }

  .tech-tag {
    background: var(--surface2);
    border: 1px solid var(--border);
    color: var(--accent2);
    font-size: 11px;
    padding: 4px 12px;
    border-radius: 3px;
    letter-spacing: 0.05em;
  }

  /* Sections */
  .section {
    margin-bottom: 48px;
    animation: fadeUp 0.6s ease both;
  }

  .section:nth-child(2) { animation-delay: 0.1s; }
  .section:nth-child(3) { animation-delay: 0.2s; }
  .section:nth-child(4) { animation-delay: 0.3s; }
  .section:nth-child(5) { animation-delay: 0.4s; }

  .section-label {
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    gap: 10px;
  }

  .section-label::after {
    content: '';
    flex: 1;
    height: 1px;
    background: var(--border);
  }

  h2 {
    font-family: 'Syne', sans-serif;
    font-size: 22px;
    font-weight: 700;
    margin-bottom: 16px;
    color: var(--text);
  }

  /* Dataset card */
  .dataset-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 24px;
    position: relative;
    overflow: hidden;
  }

  .dataset-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--accent), var(--accent2));
  }

  .dataset-meta {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 16px;
    margin-bottom: 20px;
  }

  .meta-item {
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 14px 16px;
  }

  .meta-label {
    font-size: 10px;
    color: var(--muted);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin-bottom: 6px;
  }

  .meta-value {
    font-size: 15px;
    font-weight: 500;
    color: var(--accent2);
  }

  .dataset-link {
    font-size: 12px;
    color: var(--accent2);
    text-decoration: none;
    opacity: 0.8;
    word-break: break-all;
    display: block;
    margin-top: 8px;
  }

  .dataset-link:hover { opacity: 1; text-decoration: underline; }

  /* Pipeline steps */
  .pipeline {
    display: flex;
    flex-direction: column;
    gap: 16px;
  }

  .step {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px 24px;
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 20px;
    align-items: start;
    transition: border-color 0.2s;
  }

  .step:hover { border-color: rgba(74,158,255,0.3); }

  .step-num {
    width: 32px;
    height: 32px;
    background: linear-gradient(135deg, var(--accent), var(--accent3));
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-family: 'Syne', sans-serif;
    font-size: 13px;
    font-weight: 800;
    color: white;
    flex-shrink: 0;
  }

  .step-title {
    font-family: 'Syne', sans-serif;
    font-size: 15px;
    font-weight: 700;
    margin-bottom: 8px;
  }

  .step-desc {
    font-size: 12px;
    color: var(--muted);
    line-height: 1.8;
  }

  .step-desc li {
    list-style: none;
    padding-left: 14px;
    position: relative;
  }

  .step-desc li::before {
    content: '›';
    position: absolute;
    left: 0;
    color: var(--accent);
  }

  /* Stats table */
  .stats-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
    margin-top: 12px;
  }

  .stats-table th {
    text-align: left;
    padding: 8px 16px;
    background: var(--surface2);
    color: var(--muted);
    font-size: 10px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    border: 1px solid var(--border);
  }

  .stats-table td {
    padding: 10px 16px;
    border: 1px solid var(--border);
    color: var(--text);
  }

  .stats-table td:last-child {
    color: var(--green);
    font-weight: 500;
  }

  .stats-table tr:hover td {
    background: var(--surface2);
  }

  /* Goal section */
  .goal-card {
    background: linear-gradient(135deg, rgba(232,93,74,0.08), rgba(74,158,255,0.08));
    border: 1px solid rgba(232,93,74,0.2);
    border-radius: 8px;
    padding: 28px;
    font-size: 14px;
    line-height: 1.8;
    color: var(--text);
  }

  /* Team */
  .team-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
    gap: 12px;
  }

  .team-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px;
    transition: border-color 0.2s, transform 0.2s;
  }

  .team-card:hover {
    border-color: rgba(167,139,250,0.4);
    transform: translateY(-2px);
  }

  .team-avatar {
    width: 36px;
    height: 36px;
    background: linear-gradient(135deg, var(--accent3), var(--accent2));
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-family: 'Syne', sans-serif;
    font-size: 14px;
    font-weight: 800;
    color: white;
    margin-bottom: 10px;
  }

  .team-name {
    font-size: 12px;
    font-weight: 500;
    color: var(--text);
    line-height: 1.4;
  }

  .team-role {
    font-size: 10px;
    color: var(--muted);
    margin-top: 4px;
  }

  /* Course badge */
  .course-badge {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 10px 16px;
    font-size: 12px;
    color: var(--muted);
    margin-bottom: 20px;
  }

  .course-badge span { color: var(--accent3); }

  @keyframes fadeUp {
    from { opacity: 0; transform: translateY(20px); }
    to { opacity: 1; transform: translateY(0); }
  }
</style>
</head>
<body>
<div class="container">

  <!-- Header -->
  <div class="header">
    <div class="badge">IT462 · Big Data Systems</div>
    <h1>Crime Count<br><span>Forecasting</span></h1>
    <p class="subtitle">A distributed crime prediction system built on Apache Spark that forecasts weekly crime incidents per district across Chicago to support data-driven public safety planning.</p>
    <div class="tech-stack">
      <span class="tech-tag">Apache Spark</span>
      <span class="tech-tag">Spark SQL</span>
      <span class="tech-tag">Spark MLlib</span>
      <span class="tech-tag">Scala</span>
      <span class="tech-tag">Distributed Processing</span>
    </div>
  </div>

  <!-- Dataset -->
  <div class="section">
    <div class="section-label">Dataset</div>
    <div class="dataset-card">
      <div class="dataset-meta">
        <div class="meta-item">
          <div class="meta-label">Period</div>
          <div class="meta-value">2021–2024</div>
        </div>
        <div class="meta-item">
          <div class="meta-label">Records</div>
          <div class="meta-value">971,865</div>
        </div>
        <div class="meta-item">
          <div class="meta-label">File Size</div>
          <div class="meta-value">~330 MB</div>
        </div>
        <div class="meta-item">
          <div class="meta-label">Format</div>
          <div class="meta-value">CSV</div>
        </div>
      </div>
      <p style="font-size:12px; color: var(--muted); margin-bottom:8px;">Source: Chicago Open Data Portal — contains spatial, temporal, and categorical attributes such as district, crime type, location, and timestamps.</p>
      <a class="dataset-link" href="https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2" target="_blank">↗ data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present</a>
      <div style="margin-top:16px; padding-top:16px; border-top:1px solid var(--border);">
        <div style="font-size:11px; color:var(--muted); margin-bottom:6px; letter-spacing:0.05em;">PREPROCESSED DATASET</div>
        <p style="font-size:12px; color:var(--muted);">Aggregated weekly crime counts per district with engineered features. Hosted separately due to file size.</p>
        <a class="dataset-link" href="https://drive.google.com/drive/folders/1WQXSP-ZE23fDB3HHBddJOebZY1Yfe1zI?usp=share_link" target="_blank">↗ Download via Google Drive</a>
      </div>
    </div>
  </div>

  <!-- Pipeline -->
  <div class="section">
    <div class="section-label">Data Processing Pipeline</div>
    <div class="pipeline">

      <div class="step">
        <div class="step-num">1</div>
        <div>
          <div class="step-title">Data Cleaning</div>
          <ul class="step-desc">
            <li>Removed records with missing geographic information</li>
            <li>Verified valid date range (2021–2024)</li>
            <li>Checked for and removed duplicate records</li>
          </ul>
          <table class="stats-table" style="margin-top:14px;">
            <tr><th>Stage</th><th>Rows</th></tr>
            <tr><td>Original dataset</td><td>971,865</td></tr>
            <tr><td>After cleaning</td><td>952,563</td></tr>
          </table>
        </div>
      </div>

      <div class="step">
        <div class="step-num">2</div>
        <div>
          <div class="step-title">Data Reduction</div>
          <ul class="step-desc">
            <li>Selected key features: date, district, primary_type</li>
            <li>Aggregated crime incidents weekly per district</li>
            <li>Pivoted crime types into numerical features</li>
          </ul>
          <table class="stats-table" style="margin-top:14px;">
            <tr><th>Stage</th><th>Rows</th><th>Columns</th></tr>
            <tr><td>Original dataset</td><td>971,865</td><td>22</td></tr>
            <tr><td>After reduction</td><td>4,667</td><td>33</td></tr>
          </table>
        </div>
      </div>

      <div class="step">
        <div class="step-num">3</div>
        <div>
          <div class="step-title">Data Transformation</div>
          <ul class="step-desc">
            <li>Engineered month and week_no for seasonal patterns</li>
            <li>Encoded district using StringIndexer</li>
            <li>Created lag feature prev_week_total for crime momentum</li>
          </ul>
        </div>
      </div>

    </div>
  </div>

  <!-- Goal -->
  <div class="section">
    <div class="section-label">Project Goal</div>
    <div class="goal-card">
      Build a <strong style="color:var(--accent2)">regression model</strong> that forecasts weekly crime counts per district — enabling better resource allocation and proactive crime prevention through <strong style="color:var(--accent)">data-driven public safety planning</strong>.
    </div>
  </div>

  <!-- Team -->
  <div class="section">
    <div class="section-label">Team</div>
    <div class="team-grid">
      <div class="team-card">
        <div class="team-avatar">RA</div>
        <div class="team-name">Raghad Fares Almutairi</div>
        <div class="team-role">Team Member</div>
      </div>
      <div class="team-card" style="border-style:dashed; opacity:0.5;">
        <div class="team-avatar" style="background: var(--border);">?</div>
        <div class="team-name" style="color:var(--muted);">Team Member</div>
        <div class="team-role">Add name</div>
      </div>
      <div class="team-card" style="border-style:dashed; opacity:0.5;">
        <div class="team-avatar" style="background: var(--border);">?</div>
        <div class="team-name" style="color:var(--muted);">Team Member</div>
        <div class="team-role">Add name</div>
      </div>
      <div class="team-card" style="border-style:dashed; opacity:0.5;">
        <div class="team-avatar" style="background: var(--border);">?</div>
        <div class="team-name" style="color:var(--muted);">Team Member</div>
        <div class="team-role">Add name</div>
      </div>
    </div>
  </div>

</div>
</body>
</html>
