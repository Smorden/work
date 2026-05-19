WITH base AS (
    SELECT
        supplier_id AS supplier_id,
        po_order_code AS po_order_code,
        sku AS sku,
        po_line_confirm_date AS order_confirm_date,
        po_line_fixed_last_sign_date AS last_sign_date,
        purchase_qty AS purchase_qty,
        DATEDIFF(po_line_fixed_last_sign_date, po_line_confirm_date) AS lead_time_days
    FROM dws.dws_pcct_demand_to_transit_shelf_demand_line_df
    WHERE po_line_fixed_last_sign_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
      AND is_po_delivery_finished = 1
      AND supplier_id IS NOT NULL
      AND po_line_confirm_date IS NOT NULL
      AND po_line_fixed_last_sign_date IS NOT NULL
      AND po_line_confirm_date <> '1970-01-01'
      AND po_line_fixed_last_sign_date <> '1970-01-01'
),
hard_clean AS (
    SELECT *
    FROM base
    WHERE lead_time_days >= 0
),
grp_stats AS (
    SELECT
        supplier_id,
        sku,
        COUNT(*) AS raw_sample_n,
        percentile(CAST(lead_time_days AS DOUBLE), 0.25) AS q1,
        percentile(CAST(lead_time_days AS DOUBLE), 0.75) AS q3
    FROM hard_clean
    GROUP BY supplier_id, sku
),
tagged AS (
    SELECT
        a.supplier_id,
        a.po_order_code,
        a.sku,
        a.order_confirm_date,
        a.last_sign_date,
        a.purchase_qty,
        a.lead_time_days,
        b.raw_sample_n,
        b.q1,
        b.q3,
        (b.q3 - b.q1) AS iqr,
        (b.q1 - 1.5 * (b.q3 - b.q1)) AS iqr_lower,
        (b.q3 + 1.5 * (b.q3 - b.q1)) AS iqr_upper,
        CASE
            WHEN a.lead_time_days BETWEEN (b.q1 - 1.5 * (b.q3 - b.q1))
                                     AND (b.q3 + 1.5 * (b.q3 - b.q1))
            AND a.lead_time_days <= 60
            THEN 1 ELSE 0
        END AS is_iqr_kept
    FROM hard_clean a
    JOIN grp_stats b
      ON a.supplier_id = b.supplier_id
     AND a.sku = b.sku
    WHERE b.raw_sample_n >= 3
)
SELECT
    date_sub(curdate(), interval 1 day) as dt,
    supplier_id,
    sku,
    MAX(raw_sample_n) AS raw_sample_n,
    SUM(is_iqr_kept) AS clean_sample_n,
    ceiling(AVG(IF(is_iqr_kept = 1, lead_time_days, NULL))) AS avg_delivery_period
FROM tagged
GROUP BY supplier_id, sku
HAVING SUM(is_iqr_kept) >= 3
;