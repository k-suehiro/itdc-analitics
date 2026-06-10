// ITDC Analytics v1.1.1
// - 品名ビューに製番列を追加
// - 行絞り込みで製番も検索対象に
// - 42期を他期同様7月〜6月の通期（12ヶ月）に統一

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

    // rd以外はTD除外データも取得しクライアント側トグルに使用
    // 稼働率ビューはTD除外版SQL、通常ビューはTD製番のみSQL
    const needsTdSplit = viewType !== 'rd' && viewType !== 'rd_product_name';
    let tdData = null;
    if (needsTdSplit) {
      const isUtil = viewType === 'utilization';
      const tdSql  = isUtil
        ? buildUtilizationSql(viewType, period, true)
        : buildSql(viewType, period, true);
      tdData = runBigQuery(tdSql);
    }

    return { success: true, sql: sql, data: result, tdData: tdData };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// 期・半期の設定を一元管理
function getPeriodConfig(period) {
  const all = {
    // 通期
    '39':    { startYear: 2022, startMonth:  7, count: 12, start: '2022-07-01', end: '2023-06-30' },
    '40':    { startYear: 2023, startMonth:  7, count: 12, start: '2023-07-01', end: '2024-06-30' },
    '41':    { startYear: 2024, startMonth:  7, count: 12, start: '2024-07-01', end: '2025-06-30' },
    '42':    { startYear: 2025, startMonth:  7, count: 12, start: '2025-07-01', end: '2026-06-30' },
    // 前半（7月〜12月）
    '39_H1': { startYear: 2022, startMonth:  7, count:  6, start: '2022-07-01', end: '2022-12-31' },
    '40_H1': { startYear: 2023, startMonth:  7, count:  6, start: '2023-07-01', end: '2023-12-31' },
    '41_H1': { startYear: 2024, startMonth:  7, count:  6, start: '2024-07-01', end: '2024-12-31' },
    '42_H1': { startYear: 2025, startMonth:  7, count:  6, start: '2025-07-01', end: '2025-12-31' },
    // 後半（1月〜6月）
    '39_H2': { startYear: 2023, startMonth:  1, count:  6, start: '2023-01-01', end: '2023-06-30' },
    '40_H2': { startYear: 2024, startMonth:  1, count:  6, start: '2024-01-01', end: '2024-06-30' },
    '41_H2': { startYear: 2025, startMonth:  1, count:  6, start: '2025-01-01', end: '2025-06-30' },
    '42_H2': { startYear: 2026, startMonth:  1, count:  6, start: '2026-01-01', end: '2026-06-30' },
  };
  return all[period];
}

function getMonths(period) {
  const c = getPeriodConfig(period);
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
  if (viewType === 'utilization') {
    return buildUtilizationSql(viewType, period);
  }

  const { start, end } = getPeriodConfig(period);
  const dates  = { start, end };
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

    product_name: {
      expr:    "COALESCE(product_name, '未設定')",
      label:   '品名',
      where:   '',
      orderBy: 'dim, job_id',
      jobIdCol: true
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
    },

    // 研究開発（= TD製番）を品名別に集計（区分は問わずTD全件）
    rd_product_name: {
      expr:    "COALESCE(product_name, '未設定')",
      label:   '品名',
      where:   "AND LEFT(job_id, 2) = 'TD'",
      orderBy: 'dim, job_id',
      jobIdCol: true
    }
  };

  const dim = dimConfigs[viewType];
  // tdOnly モード: TD製番のみに絞る（rd は dim.where で既にTD限定）
  const tdFilter = (tdOnly && viewType !== 'rd' && viewType !== 'rd_product_name')
    ? "AND LEFT(job_id, 2) = 'TD'"
    : '';

  const monthCols = months.map(m =>
    `ROUND(SUM(CASE WHEN FORMAT_DATE('%Y-%m', report_date) = '${m}' THEN hours ELSE 0 END), 1) AS \`${m}\``
  ).join(',\n    ');

  const withJobId = !!dim.jobIdCol;
  const baseSelect = withJobId
    ? `report_date,\n    hours,\n    ${dim.expr} AS dim,\n    COALESCE(job_id, '未設定') AS job_id`
    : `report_date,\n    hours,\n    ${dim.expr} AS dim`;
  const selectDims = withJobId
    ? `dim AS \`${dim.label}\`,\n  job_id AS \`製番\`,`
    : `dim AS \`${dim.label}\`,`;
  const groupBy = withJobId ? 'dim, job_id' : 'dim';

  return `WITH base AS (
  SELECT
    ${baseSelect}
  FROM \`itdc-wdr.daily_reports.reports\`
  WHERE report_date BETWEEN '${dates.start}' AND '${dates.end}'
  AND job_id IS NOT NULL
  AND job_id != ''
  ${tdFilter}
  ${dim.where}
)
SELECT
  ${selectDims}
  ${monthCols},
  ROUND(SUM(hours), 1) AS \`合計\`
FROM base
GROUP BY ${groupBy}
ORDER BY ${dim.orderBy}`;
}

// ─── 稼働率（社員別）専用ビルダー ──────────────────────────────────────────
// tdExclude=true のとき TD製番を分子から除外（TD除外 = 役務稼働率と同値）
function buildUtilizationSql(viewType, period, tdExclude) {
  tdExclude = !!tdExclude;
  const { start, end } = getPeriodConfig(period);
  const dates  = { start, end };
  const months = getMonths(period);

  // TD含む: 全製番の時間 / 総時間
  // TD除外: TD以外の製番の時間 / 総時間
  const numerator = tdExclude
    ? "SUM(CASE WHEN job_id IS NOT NULL AND job_id != '' AND LEFT(job_id, 2) != 'TD' THEN hours ELSE 0 END)"
    : "SUM(CASE WHEN job_id IS NOT NULL AND job_id != ''                              THEN hours ELSE 0 END)";

  const denominator = "SUM(hours)";

  const monthCols = months.map(m =>
    `ROUND(MAX(CASE WHEN ym = '${m}' THEN SAFE_DIVIDE(num_hours, total_hours) * 100 END), 1) AS \`${m}\``
  ).join(',\n    ');

  return `WITH monthly AS (
  SELECT
    COALESCE(user_name, '未設定') AS user_name,
    FORMAT_DATE('%Y-%m', report_date) AS ym,
    ${denominator} AS total_hours,
    ${numerator}   AS num_hours
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

// ─── Gemini AI 関連 ─────────────────────────────────────────────────────────

function callGemini(prompt) {
  var apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  if (!apiKey) throw new Error('GEMINI_API_KEY がScript Propertiesに設定されていません');

  var endpoint =
    'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=' + apiKey;
  var payload = {
    contents: [{ parts: [{ text: prompt }] }],
    generationConfig: { temperature: 0.3, maxOutputTokens: 2048 }
  };
  var options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  var response = UrlFetchApp.fetch(endpoint, options);
  var code     = response.getResponseCode();
  var body     = JSON.parse(response.getContentText());
  if (code !== 200) {
    throw new Error('Gemini API エラー (' + code + '): ' + ((body.error && body.error.message) || '不明なエラー'));
  }
  return body.candidates[0].content.parts[0].text;
}

// 全列（次元＋月別＋合計）をTSV形式で返す（AI分析用）
function formatDataAsTable(schema, rows, isUtil) {
  var unit  = isUtil ? '%' : 'h';
  // ヘッダー行：月列は短縮表記
  var header = schema.map(function(col, i) {
    if (i === 0) return col;
    return col.replace(/^(\d{4})-(\d{2})$/, function(_, y, m) { return y.slice(2) + '/' + m; });
  });
  var lines = [header.join('\t')];
  rows.forEach(function(row) {
    var cells = schema.map(function(col, i) {
      if (i === 0) return row[col] || '(未設定)';
      var v = parseFloat(row[col]);
      return isNaN(v) ? '-' : v.toFixed(1) + unit;
    });
    lines.push(cells.join('\t'));
  });
  return lines.join('\n');
}

// クライアントから渡された表示データを Gemini で分析
function analyzeResult(viewType, period, includeTD, dataJson) {
  try {
    var data      = JSON.parse(dataJson);
    var periodCfg = getPeriodConfig(period);
    var isUtil    = viewType === 'utilization';
    var viewNames = {
      task_type:'作業項目別', user_name:'社員別', product_name:'品名別',
      client:'顧客別', department:'請求先部門別',
      rd:'研究開発区分別', rd_product_name:'研究開発品名別', utilization:'社員別稼働率'
    };
    var rows      = data.rows.slice(0, 30);
    var isTrunc   = data.rows.length > 30;
    var table     = formatDataAsTable(data.schema, rows, isUtil);
    var truncNote = isTrunc ? '（全' + data.rows.length + '行のうち上位30行）' : '（全' + data.rows.length + '行）';

    var prompt = [
      '# 指示',
      '前置き・自己紹介・タイトル行は一切不要。以下のフォーマットのみで即座に回答せよ。',
      '',
      '# コンテキスト',
      'ITDC（受託開発・印刷会社）の業務集計データを分析する。',
      '対象期間: ' + periodCfg.start + ' ～ ' + periodCfg.end,
      '集計軸: ' + (viewNames[viewType] || viewType) + '　集計値: ' + (isUtil ? '稼働率(%)' : '工数(時間)') + '　TD製番: ' + (includeTD ? '含む' : '除外'),
      '',
      '# 集計データ（TSV形式）' + truncNote,
      table,
      '',
      '# 回答フォーマット（日本語Markdown・400字以内）',
      '## 主要な傾向',
      '- （数値を引用して2〜3点、月次トレンドがあれば言及）',
      '',
      '## 注目点・異常値',
      '- （突出して高い/低い項目、急増/急減月など。なければ「特になし」）',
      '',
      '## 経営アドバイス',
      '- （2〜3点。具体的な行動につながる提言）'
    ].join('\n');

    return { success: true, analysis: callGemini(prompt) };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// 自然言語質問 → ルーティング → BigQuery集計 → Gemini分析
function chatQuery(userMessage) {
  try {
    var validViews   = ['task_type','user_name','product_name','client','department','rd','rd_product_name','utilization'];
    var validPeriods = ['39','40','41','42'];
    var todayStr = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd');

    // Step1: Gemini に集計パラメータを選ばせる
    var step1Prompt = [
      'あなたはITDCの業務分析ルーターです。ユーザーの質問に最適な集計パラメータをJSON形式のみで返してください。',
      '',
      '■ viewType の選択肢',
      '  "task_type":作業項目別工数  "user_name":社員別工数  "product_name":品名別工数',
      '  "client":顧客別工数  "department":請求先部門別工数',
      '  "rd":研究開発区分別  "rd_product_name":研究開発品名別  "utilization":社員別稼働率(%)',
      '',
      '■ period の選択肢: "39"〜"42"（期指定なければ最新の"42"）',
      '  "42"=42期(2025/7〜2026/6)  "41"=41期(2024/7〜2025/6)  "40"=40期  "39"=39期',
      '  今日は ' + todayStr,
      '',
      '■ 出力フォーマット（このJSONのみ・余分なテキスト不要）',
      '{"viewType":"task_type","period":"42","reason":"選択理由"}',
      '',
      '■ ユーザーの質問',
      userMessage
    ].join('\n');

    var step1Raw  = callGemini(step1Prompt);
    var jsonMatch = step1Raw.match(/\{[\s\S]*?\}/);
    if (!jsonMatch) throw new Error('Geminiのルーティング出力が不正: ' + step1Raw);
    var step1    = JSON.parse(jsonMatch[0]);
    var viewType = validViews.indexOf(step1.viewType)  !== -1 ? step1.viewType : 'task_type';
    var period   = validPeriods.indexOf(step1.period)  !== -1 ? step1.period   : '42';
    var reason   = step1.reason || '';

    // Step2: BigQuery 集計
    var sql    = buildSql(viewType, period);
    var result = runBigQuery(sql);

    // Step3: Gemini で分析・回答
    var periodCfg = getPeriodConfig(period);
    var isUtil    = viewType === 'utilization';
    var viewNames = {
      task_type:'作業項目別', user_name:'社員別', product_name:'品名別',
      client:'顧客別', department:'請求先部門別',
      rd:'研究開発区分別', rd_product_name:'研究開発品名別', utilization:'社員別稼働率'
    };
    var rows      = result.rows.slice(0, 30);
    var isTrunc   = result.rows.length > 30;
    var table     = formatDataAsTable(result.schema, rows, isUtil);
    var truncNote = isTrunc ? '（上位30行）' : '';

    var step3Prompt = [
      'あなたはITDCの業務データアナリストです。ユーザーの質問に日本語で具体的・簡潔に回答してください。',
      '',
      '【ユーザーの質問】',
      userMessage,
      '',
      '【参照したデータ】',
      '  集計軸: ' + (viewNames[viewType] || viewType) + '（選択理由: ' + reason + '）',
      '  対象期間: ' + periodCfg.start + ' ～ ' + periodCfg.end,
      '',
      '【集計結果】' + truncNote,
      table,
      '',
      '数値を引用しながら根拠のある回答をしてください（Markdown形式・400字以内）。',
      '最後に「さらに詳しく知りたい点があれば質問してください」と添えてください。'
    ].join('\n');

    return {
      success:  true,
      answer:   callGemini(step3Prompt),
      viewType: viewType,
      period:   period,
      sql:      sql,
      data:     result
    };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// ─────────────────────────────────────────────────────────────────────────────

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