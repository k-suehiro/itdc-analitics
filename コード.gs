const PROJECT_ID = 'itdc-wdr';

function doGet() {
  return HtmlService.createTemplateFromFile('index').evaluate()
      .setTitle('ITDC WDR Analytics Engine')
      .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function processQuery(viewType, period) {
  try {
    const sql = buildSql(viewType, period);
    const result = runBigQuery(sql);

    // rd・稼働率ビュー以外はTDのみデータも取得しクライアント側トグルに使用
    const needsTdSplit = viewType !== 'rd'
      && viewType !== 'utilization_yakumu'
      && viewType !== 'utilization';
    let tdData = null;
    if (needsTdSplit) {
      const tdSql = buildSql(viewType, period, true);
      tdData = runBigQuery(tdSql);
    }

    return { success: true, sql: sql, data: result, tdData: tdData };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// 期ごとの年月リストを生成（7月始まり）
function getMonths(period) {
  const config = {
    '39': { startYear: 2022, startMonth: 7, count: 12 },
    '40': { startYear: 2023, startMonth: 7, count: 12 },
    '41': { startYear: 2024, startMonth: 7, count: 12 },
    '42': { startYear: 2025, startMonth: 7, count: 9 }  // ～2026-03
  };
  const c = config[period];
  const months = [];
  for (let i = 0; i < c.count; i++) {
    const total = c.startMonth - 1 + i;
    const year  = c.startYear + Math.floor(total / 12);
    const month = (total % 12) + 1;
    months.push(year + '-' + String(month).padStart(2, '0'));
  }
  return months;
}

// tdOnly = true のとき TD製番のみに絞ったSQLを生成（クライアント側差分計算用）
function buildSql(viewType, period, tdOnly) {
  tdOnly = !!tdOnly;
  if (viewType === 'utilization_yakumu' || viewType === 'utilization') {
    return buildUtilizationSql(viewType, period);
  }

  const periodDates = {
    '39': { start: '2022-07-01', end: '2023-06-30' },
    '40': { start: '2023-07-01', end: '2024-06-30' },
    '41': { start: '2024-07-01', end: '2025-06-30' },
    '42': { start: '2025-07-01', end: '2026-03-31' }
  };
  const dates  = periodDates[period];
  const months = getMonths(period);

  const dimConfigs = {

    task_type: {
      expr:    "COALESCE(task_type, '未設定')",
      label:   '作業項目',
      where:   '',
      orderBy: 'dim'
    },

    user_name: {
      expr:    "COALESCE(user_name, '未設定')",
      label:   '社員名',
      where:   '',
      orderBy: 'dim'
    },

    client: {
      expr: `CASE
          WHEN LEFT(job_id, 2) = 'MP'
            AND (client_name LIKE '%シープラス%' OR client_name LIKE '%大野印刷%') THEN 'シープラス'
          ELSE COALESCE(client_name, '未設定')
        END`,
      label:   '顧客名',
      where:   '',
      orderBy: 'dim'
    },

    department: {
      expr: `CASE
          WHEN LEFT(job_id, 2) IN ('TK','FK')                                       THEN '東京事業所'
          WHEN LEFT(job_id, 2) = 'HM'                                               THEN '浜松事業所'
          WHEN LEFT(job_id, 2) = 'NA'                                               THEN '名古屋事業所'
          WHEN LEFT(job_id, 2) = 'MT'                                               THEN '松本事業所'
          WHEN LEFT(job_id, 2) = 'OS'                                               THEN '大阪事業所'
          WHEN LEFT(job_id, 2) = 'SL'                                               THEN '国内支援室CSC'
          WHEN LEFT(job_id, 2) = 'MP'
            AND (client_name LIKE '%シープラス%' OR client_name LIKE '%大野印刷%') THEN 'シープラス'
          WHEN LEFT(job_id, 2) = 'MP'                                               THEN '海外事業部'
          WHEN LEFT(job_id, 2) = 'TP'                                               THEN 'プロダクションセンター'
          WHEN LEFT(job_id, 2) = 'EK' THEN
            CASE
              WHEN COALESCE(REGEXP_EXTRACT(product_name, r'【(.+?)】'), '本社')
                   IN ('本社', '人事総務課') THEN '人事総務部'
              ELSE REGEXP_EXTRACT(product_name, r'【(.+?)】')
            END
          ELSE CONCAT('その他(', LEFT(job_id, 2), ')')
        END`,
      label: '請求先部門',
      where: '',
      orderBy: `CASE dim
          WHEN '東京事業所'             THEN  1
          WHEN '浜松事業所'             THEN  2
          WHEN '名古屋事業所'           THEN  3
          WHEN '松本事業所'             THEN  4
          WHEN '大阪事業所'             THEN  5
          WHEN '国内支援室CSC'          THEN  6
          WHEN '海外事業部'             THEN  7
          WHEN 'シープラス'             THEN  8
          WHEN '国内事業部'             THEN  9
          WHEN '人事総務部'             THEN 10
          WHEN '財務経理部'             THEN 11
          WHEN '役員室'                 THEN 12
          WHEN 'プロダクションセンター' THEN 13
          ELSE 99
        END`
    },

        rd: {
      expr: `CASE
          WHEN REGEXP_CONTAINS(product_name, 'ソフトウェア開発|ソフト開発') THEN 'ソフトウェア開発'
          WHEN REGEXP_CONTAINS(product_name, '研究開発')                    THEN '研究開発'
          ELSE '特別保守'
        END`,
      label:   '区分',
      where:   "AND LEFT(job_id, 2) = 'TD'",
      orderBy: 'dim'
    }
  };

  const dim = dimConfigs[viewType];
  // tdOnly モード: TD製番のみに絞る（rd は dim.where で既にTD限定）
  const tdFilter = (tdOnly && viewType !== 'rd') ? "AND LEFT(job_id, 2) = 'TD'" : '';

  const monthCols = months.map(m =>
    `ROUND(SUM(CASE WHEN FORMAT_DATE('%Y-%m', report_date) = '${m}' THEN hours ELSE 0 END), 1) AS \`${m}\``
  ).join(',\n    ');

  return `WITH base AS (
  SELECT
    report_date,
    hours,
    ${dim.expr} AS dim
  FROM \`itdc-wdr.daily_reports.reports\`
  WHERE report_date BETWEEN '${dates.start}' AND '${dates.end}'
  AND job_id IS NOT NULL
  AND job_id != ''
  ${tdFilter}
  ${dim.where}
)
SELECT
  dim AS \`${dim.label}\`,
  ${monthCols},
  ROUND(SUM(hours), 1) AS \`合計\`
FROM base
GROUP BY dim
ORDER BY ${dim.orderBy}`;
}

// ─── 稼働率・役務稼働率（社員別）専用ビルダー ───────────────────────────────
function buildUtilizationSql(viewType, period) {
  const periodDates = {
    '39': { start: '2022-07-01', end: '2023-06-30' },
    '40': { start: '2023-07-01', end: '2024-06-30' },
    '41': { start: '2024-07-01', end: '2025-06-30' },
    '42': { start: '2025-07-01', end: '2026-03-31' }
  };
  const dates  = periodDates[period];
  const months = getMonths(period);

  // 役務稼働率: TD製番を除くjob_id有り時間 / 総時間（定義固定）
  // 稼働率　　: job_id有り時間 / 総時間
  const isYakumu  = viewType === 'utilization_yakumu';
  const numerator = isYakumu
    ? "SUM(CASE WHEN job_id IS NOT NULL AND job_id != '' AND LEFT(job_id, 2) != 'TD' THEN hours ELSE 0 END)"
    : "SUM(CASE WHEN job_id IS NOT NULL AND job_id != ''                              THEN hours ELSE 0 END)";

  const monthCols = months.map(m =>
    `ROUND(MAX(CASE WHEN ym = '${m}' THEN SAFE_DIVIDE(num_hours, total_hours) * 100 END), 1) AS \`${m}\``
  ).join(',\n    ');

  return `WITH monthly AS (
  SELECT
    COALESCE(user_name, '未設定') AS user_name,
    FORMAT_DATE('%Y-%m', report_date) AS ym,
    SUM(hours)   AS total_hours,
    ${numerator} AS num_hours
  FROM \`itdc-wdr.daily_reports.reports\`
  WHERE report_date BETWEEN '${dates.start}' AND '${dates.end}'
  GROUP BY user_name, ym
)
SELECT
  user_name AS \`社員名\`,
  ${monthCols},
  ROUND(SAFE_DIVIDE(SUM(num_hours), SUM(total_hours)) * 100, 1) AS \`合計\`
FROM monthly
GROUP BY user_name
ORDER BY user_name`;
}

function runBigQuery(sql) {
  const request = { query: sql, useLegacySql: false, timeoutMs: 30000 };
  let queryResults = BigQuery.Jobs.query(request, PROJECT_ID);
  const jobId = queryResults.jobReference.jobId;

  while (!queryResults.jobComplete) {
    Utilities.sleep(500);
    queryResults = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId);
  }

  const rows   = queryResults.rows || [];
  const schema = queryResults.schema.fields;
  const resultData = rows.map(row => {
    const rowObj = {};
    row.f.forEach((col, idx) => { rowObj[schema[idx].name] = col.v; });
    return rowObj;
  });

  return { schema: schema.map(f => f.name), rows: resultData };
}