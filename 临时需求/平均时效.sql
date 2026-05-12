/**
1.近12个月的供应商实际交期天数，供应商满足订单数>=30，字段：供应商，平均交期，中位数交期，80%分位数交期
 */
select
    supplier_name as 供应商
    , po_order_code as 订单号
    , sku
    , po_line_confirm_date as 订单确认日期
    , po_line_fixed_last_sign_date as 末次签收日期
    , purchase_qty as 采购数量
    , po_delivery_days as `交期(剔除中仓节假日)`
from dws.dws_pcct_demand_to_transit_shelf_demand_line_df
where po_line_fixed_last_sign_date >= date_sub(curdate(), interval 1 year)
and is_po_delivery_finished = 1
;
/**
2.需要近12个月的实际航线平均时效，后续可能要定期刷航线时效
字段：配送中心，仓库，下单-提货	提货-开船	开船-目的港	目的港-到仓	到仓时效	到仓-海外仓上架	下单-上架
 */
    select
        asn.ship_no as 发运调拨单,
        NVL(wh.dc_location_country, '')      AS 国家,
        wh.warehouse_cn_name as 仓库,
        wh.dc_name as 配送中心,
        asn.route_name as 航线,
        asn.move_in_qty 发运数量,
        sh.order_date as 下单日期,
        sh.actual_pickup_time as 提货日期,
        sh.actual_sailing_time as 开船日期,
        sh.actual_arrival_port_time as 到港日期,
        sh.actual_arrival_warehouse_time as 到仓日期,
        asn.oversea_in_date as 末次上架日期,
        datediff(sh.actual_pickup_time, sh.order_date) as `下单-提货天数`,
        datediff(sh.actual_sailing_time, sh.actual_pickup_time) as `提货-开船天数`,
        datediff(sh.actual_arrival_port_time, sh.actual_sailing_time) as `开船-到港天数`,
        datediff(sh.actual_arrival_warehouse_time, sh.actual_arrival_port_time) as `到港-到仓天数`,
        datediff(asn.oversea_in_date, sh.actual_arrival_warehouse_time) as `到仓-上架天数`,
        datediff(asn.oversea_in_date, sh.order_date) as `下单-上架天数`
    FROM dws.dws_ivct_transfer_out_to_oversea_in_ship_di AS asn
    LEFT JOIN dwd.dwd_dim_warehouse_df AS wh ON wh.warehouse_id = asn.destination_warehouse_id
    left join dws.dws_ivct_first_trip_flow_ship_no_df as sh on asn.ship_no = sh.ship_no
    WHERE asn.oversea_in_date >= date_sub(curdate(), interval 1 year)
        AND asn.oversea_in_qty >= asn.move_in_qty
        and asn.ship_no <> 'AL1230901027'
        and asn.move_estimate_oversea_in_date <> '1970-01-01'
        and asn.route_type = 1
 ;