package com.alibaba.otter.canal.client.adapter.es.test.sync;

import java.util.Arrays;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.es.ESAdapter;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.es.config.SqlParser;
import com.alibaba.otter.canal.client.adapter.support.Dml;

public class MyTest {
	private ESAdapter esAdapter;

	@Before
	public void init() {
		// AdapterConfigs.put("es", "mytest_user_single.yml");
		esAdapter = Common.init();
	}

	/**
	 * 单表插入
	 */
	@Test
	public void test01() {
		String ss = "{\"data\":[{\"order_id\":171452802001,\"out_order_id\":\"\",\"shop_id\":4,\"member_id\":4293428,\"member_name\":\"222944lo2517ppp1@email\",\"ship_status\":0,\"pay_status\":0,\"order_status\":\"dead\",\"ext_status\":\"WAIT_VERIFY\",\"is_delivery\":1,\"payment_type\":1,\"payment\":\"\",\"shipping_id\":19,\"shipping\":\"本地配送\",\"ship_area\":\"湖南_长沙市_岳麓区_麓谷街道:43_430100000000_430104000000_430104030000\",\"ship_name\":\"张三6\",\"ship_addr\":\"湖南省长沙市岳麓区青春大道附近\",\"ship_zip\":\"\",\"ship_tel\":\"\",\"ship_email\":\"1-V0WZA-1@bigMember.com\",\"ship_time\":\"及时达,预计明天11:30送达\",\"ship_mobile\":\"13667381314\",\"is_tax\":0,\"tax_type\":null,\"tax_title\":\"\",\"tax_content\":\"\",\"weight\":10.0,\"ip\":\"218.76.52.6\",\"is_protect\":0,\"cost_protect\":0,\"use_score\":0,\"gain_score\":0,\"total_use_envelope\":0,\"total_sale_amount\":160,\"total_taxation\":0,\"total_real_taxation\":0,\"total_pay_amount\":660,\"total_real_pay_amount\":0,\"total_deduct_amount\":0,\"express_fee\":500,\"product_promotion_amount\":0,\"order_promotion_amount\":0,\"coupon_id\":\"\",\"coupon_amount\":0,\"coupon_add_income\":\"\",\"product_num\":1,\"memo\":\"\",\"mark_type\":null,\"mark_text\":\"\",\"order_refer\":\"normal\",\"platform_source\":\"super_ios\",\"allow_return\":\"0\",\"is_split\":\"0\",\"group_id\":\"\",\"cancel_reason\":\"订单付款超时取消\",\"order_create_time\":1495725973556,\"cancel_time\":1495733185053,\"cancel_time_out\":7200000,\"send_time\":null,\"sign_time\":null,\"order_promotion_detail\":\"{\\\"shopId\\\":4,\\\"products\\\":{\\\"29687001550\\\":{\\\"price\\\":160,\\\"originPrice\\\":160,\\\"promptPrice\\\":160,\\\"num\\\":1,\\\"splitOrderInfo\\\":{\\\"171452802001\\\":1},\\\"amount\\\":{\\\"total\\\":160,\\\"discount\\\":0,\\\"orderDiscount\\\":0,\\\"bargin\\\":160,\\\"pay\\\":160},\\\"gifts\\\":[],\\\"promotions\\\":[],\\\"coupons\\\":[],\\\"extra\\\":{},\\\"select\\\":true,\\\"toolFilter\\\":[]}},\\\"amount\\\":{\\\"total\\\":160,\\\"discount\\\":0,\\\"orderDiscount\\\":0,\\\"bargin\\\":160,\\\"pay\\\":160},\\\"gifts\\\":[],\\\"promotions\\\":[],\\\"coupons\\\":[]}\",\"bank_promotion_detail\":\"\",\"jl_saler\":\"\",\"saler\":\"\",\"order_type\":10,\"shop_type\":3,\"biz_type\":0,\"provider_bn\":\"\",\"self_id\":null,\"real_name\":\"\",\"id_card\":\"\",\"front_img\":\"\",\"reverse_img\":\"\",\"sync_status\":null,\"sync_time\":null,\"pay_time\":null,\"warehouse_type\":\"Shop\",\"warehouse_tag\":\"TAG_SHOP\",\"warehouse_id\":\"1\",\"order_tag\":\"CS\",\"channel_track\":\"\",\"migrate_source\":\"App Store\",\"trash_status\":2,\"refund_amount\":null,\"refund_envelope\":0,\"claim_amount\":null,\"refund_express_fee\":null,\"create_time\":1495725973556,\"last_modified_time\":1496649472839,\"device_id\":\"7df5f970d4b8515\",\"hs_record_no\":\"\",\"hs_area_code\":\"\",\"hs_record_name\":\"\",\"swc_code\":\"\",\"order_dtc_status\":\"\",\"taxation_type\":0,\"allow_apply_cancel\":0,\"order_parent_id\":701452802001,\"split_type\":0,\"show_status\":1,\"ztm_code\":\"\",\"settle_amount\":null,\"total_real_pay_amount2\":null,\"delivery_time\":0,\"express_company_name\":\"\",\"sign\":null,\"express_company_code\":\"\"}],\"database\":\"bbg_plat_trade\",\"destination\":\"trade\",\"es\":1557390062000,\"groupId\":null,\"isDdl\":false,\"old\":[{\"ship_name\":\"张三5\"}],\"pkNames\":[\"order_id\"],\"sql\":\"\",\"table\":\"bbg_b2c_orders\",\"ts\":1557418872757,\"type\":\"UPDATE\"}";
		Dml dml = JSON.parseObject(ss, Dml.class);

//		String database = dml.getDatabase();
//		String table = dml.getTable();
//		Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

		Map<String, ESSyncConfig> configMap = ESSyncConfigLoader.load(null);
		
		ESSyncConfig config = new ESSyncConfig();
		config.setDataSourceKey("tradeDS");
		config.setDestination("trade");
		config.setGroupId("g1");
		
		ESMapping esMapping = new ESMapping();
		esMapping.set_index("bbg_orders");
		esMapping.set_type("_doc");
		esMapping.set_id("_id");
		esMapping.setUpsert(true);
		esMapping.setSql("select a.order_id as _id,a.order_id,a.shop_id,a.member_id,a.member_name,a.ship_name,a.ship_addr from bbg_b2c_orders a");
		esMapping.setEtlCondition("where order_id>='{0}' and order_id<'{1}'");
		esMapping.setCommitBatch(3000);
		esMapping.setSchemaItem(SqlParser.parse(esMapping.getSql()));
		config.setEsMapping(esMapping);
//		SqlParser.parse(config.getEsMapping().getSql());
		configMap.put("trade", config);
		esAdapter.getEsSyncService().sync(configMap.values(), dml);

		GetResponse response = esAdapter.getTransportClient().prepareGet("bbg_orders", "_doc", "171452802001").get();
		Assert.assertEquals("Eric", response.getSource().get("_name"));
	}
	
	@Test
	public void test02() {
		String ss = "{\"data\":[{\"id\":24547,\"channel\":\"JD\",\"sku_bn\":\"105824727\",\"sku_name\":\"贝涛8寸玩偶\",\"safety_stock\":null,\"out_sku_id\":2021770360,\"out_shop_category_id\":4638359,\"out_shop_category_name\":\"日用家纺\",\"out_first_category_id\":21929,\"out_first_category_name\":\"玩具\",\"out_second_category_id\":21930,\"out_second_category_name\":\"毛绒玩具\",\"out_third_category_id\":21938,\"out_third_category_name\":\"其他毛绒\",\"out_brand_id\":35247,\"out_brand_name\":\"其他品牌\",\"serial_no\":103405,\"remark\":\"\",\"gmt_create\":1547626872147,\"gmt_modified\":1557476580018,\"available\":\"A\"}],\"database\":\"bbg_plat_goods\",\"destination\":\"goods\",\"es\":1557476580000,\"groupId\":null,\"isDdl\":false,\"old\":[{\"serial_no\":103403,\"gmt_modified\":1557476280016}],\"pkNames\":[\"id\"],\"sql\":\"\",\"table\":\"gc_channel_sku\",\"ts\":1557505390327,\"type\":\"UPDATE\"}";
//		String ss = "{\"data\":[{\"id\":228086,\"channel\":\"JD\",\"md_code\":\"120153\",\"product_bn\":\"101125013\",\"prompt_price\":1294,\"prompt_code\":\"02\",\"begin_time\":1543593600000,\"end_time\":1546185599000,\"bill_no\":\"test\",\"prompt_type\":\"1\",\"out_prompt_id\":null,\"serial_no\":982885,\"remark\":\"\",\"gmt_create\":1528964294528,\"gmt_modified\":1557804601445}],\"database\":\"bbg_plat_goods\",\"destination\":\"goods\",\"es\":1557804601000,\"groupId\":null,\"isDdl\":false,\"old\":[{\"prompt_price\":1293,\"gmt_modified\":1557802896121}],\"pkNames\":[\"id\"],\"sql\":\"\",\"table\":\"gc_channel_prompt_product\",\"ts\":1557833411964,\"type\":\"UPDATE\"}";
//		String ss = "{\"data\":[{\"id\":656231,\"channel\":\"BTGO\",\"md_code\":\"120036\",\"product_bn\":\"107263775\",\"normal_price\":4992,\"synchronizable\":\"A\",\"available\":\"A\",\"available_type\":\"A\",\"serial_no\":2690985,\"remark\":\"\",\"gmt_create\":1547711000015,\"gmt_modified\":1557676085674}],\"database\":\"bbg_plat_goods\",\"destination\":\"goods\",\"es\":1557676085000,\"groupId\":null,\"isDdl\":false,\"old\":[{\"normal_price\":4991,\"gmt_modified\":1557676058951}],\"pkNames\":[\"id\"],\"sql\":\"\",\"table\":\"gc_channel_product\",\"ts\":1557704896538,\"type\":\"UPDATE\"}";
//		String ss = "{\"data\":[{\"id\":9361,\"product_bn\":\"104248035\",\"md_code\":\"120236\",\"warehouse_id\":1001,\"channel\":\"BTGO\",\"store\":7,\"nostore\":0,\"freez\":2,\"lock\":0,\"nostore_sell\":1,\"store_type\":1,\"serial_no\":54012746,\"remark\":\"\",\"record_create_time\":1534931458000,\"record_last_modify\":1557975651000}],\"database\":\"bbg_plat_sp\",\"destination\":\"store\",\"es\":1557975651000,\"groupId\":null,\"isDdl\":false,\"old\":[{\"freez\":1,\"record_last_modify\":1547354403000}],\"pkNames\":[\"id\"],\"sql\":\"\",\"table\":\"sp_store_o2o_details\",\"ts\":1558004462493,\"type\":\"UPDATE\"}";
		Dml dml = JSON.parseObject(ss, Dml.class);

//		String database = dml.getDatabase();
//		String table = dml.getTable();
//		Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

		Map<String, ESSyncConfig> configMap = ESSyncConfigLoader.load(null);
		
		ESSyncConfig config = new ESSyncConfig();
		config.setDataSourceKey("goodsDS");
		config.setDestination("goods");
		config.setGroupId("g2");
//		config.setDataSourceKey("storeDS");
//		config.setDestination("store");
//		config.setGroupId("g3");
		
		ESMapping esMapping = new ESMapping();
		esMapping.set_index("channel_goods");
		esMapping.set_type("_doc");
		esMapping.set_id("_id");
		esMapping.setUpsert(true);
//		esMapping.setUpdateFields(Arrays.asList("actualStore"));
//		esMapping.setForceUpdate(true);
//		esMapping.setSql("select concat(a.md_code,'_',a.product_bn,'_',a.channel) as _id,a.id as channel_product_id,a.product_bn,a.md_code,a.channel,a.normal_price,GROUP_CONCAT(b.id order by b.id desc separator ';') AS prompt_ids,GROUP_CONCAT(b.prompt_price order by b.id desc separator ';') AS prompt_prices,GROUP_CONCAT(b.prompt_code order by b.id desc separator ';') AS prompt_codes,GROUP_CONCAT(b.bill_no order by b.id desc separator ';') AS prompt_bill_nos,c.id as channel_sku_id,c.sku_name from gc_channel_product a left join gc_channel_prompt_product b on a.md_code = b.md_code and a.channel = b.channel and a.product_bn = b.product_bn left join gc_channel_sku c on a.product_bn = c.sku_bn and a.channel = c.channel");
		String sql = "select concat(a.channel,'_',a.md_code,'_',a.product_bn) as _id,a.id as channelProductId,a.channel,a.md_code as mdCode,a.product_bn as productBn,a.normal_price as normalPrice,a.synchronizable,a.available as channelProductAvailable,a.available_type as channelProductAvailableType,a.serial_no as channelProductSerialNo,a.remark as channelProductRemark,c.id AS channelSkuId,c.safety_stock as safetyStock,c.out_sku_id as outSkuId,c.out_shop_category_id as outShopCategoryId,c.out_shop_category_name as outShopCategoryName,c.out_first_category_id as outFirstCategoryId,c.out_first_category_name as outFirstCategoryName,c.out_second_category_id as outSecondCategoryId,c.out_second_category_name as outSecondCategoryName,c.out_third_category_id as outThirdCategoryId,c.out_third_category_name as outThirdCategoryName,c.out_brand_id as outBrandId,c.out_brand_name as outBrandName,c.serial_no as channelSkuSerialNo,c.remark as channelSkuRemark,c.available as channelSkuAvailable from gc_channel_product a LEFT JOIN gc_channel_sku c ON a.product_bn = c.sku_bn AND a.channel = c.channel";
//		String sql = "select concat(a.channel,'_',a.md_code,'_',a.product_bn) as _id,a.id as channel_product_id,a.channel,a.md_code,a.product_bn,max(case when b.prompt_type='1' and b.prompt_code!='01' then b.id else -1 end) as channel_prompt_id,max(case when b.prompt_type='1' and b.prompt_code!='01' then b.prompt_price else -1 end) as prompt_price,(case when b.prompt_type='1' and b.prompt_code!='01' then b.prompt_code else -1 end) as prompt_code,(case when b.prompt_type='1' and b.prompt_code!='01' then date_format(b.begin_time, '%Y-%m-%d %H:%i:%s') else -1 end) as begin_time,(case when b.prompt_type='1' and b.prompt_code!='01' then date_format(b.end_time, '%Y-%m-%d %H:%i:%s') else -1 end) as end_time,(case when b.prompt_type='1' and b.prompt_code!='01' then b.bill_no else -1 end) as bill_no,(case when b.prompt_type='1' and b.prompt_code!='01' then b.prompt_type else -1 end) as prompt_type,(case when b.prompt_type='1' and b.prompt_code!='01' then b.out_prompt_id else -1 end) as out_prompt_id,(case when b.prompt_type='1' and b.prompt_code!='01' then b.serial_no else -1 end) as channel_prompt_serial_no,(case when b.prompt_type='1' and b.prompt_code!='01' then b.remark else -1 end) as channel_prompt_remark from gc_channel_product a left join gc_channel_prompt_product b on a.md_code = b.md_code and a.channel = b.channel and a.product_bn = b.product_bn";
//		String sql = "select concat(a.channel,'_',a.md_code,'_',a.product_bn) as _id,a.id as storeid,a.channel,a.md_code as mdcode,a.product_bn as productbn,a.store,a.nostore,a.freez,a.lock,a.nostore_sell as nostoresell,a.store_type as storetype,a.serial_no as storeserialno,a.remark as storeremark,(case when a.nostore_sell = 1 then (a.nostore-a.freez-a.lock) when a.nostore_sell = 0 then (a.store-a.freez-a.lock) else -1 end) as actualStore from sp_store_o2o_details a";
//		String sql = "select concat(a.channel,'_',a.md_code,'_',a.product_bn) as _id,a.id as channelProductId,a.product_bn as productBn,b.sku_id as skuId,b.barcode,b.unit,b.catch_weight_ind as catchWeightInd,c.spu_id as spuId,c.catback_id as spuCatbackId,d.catback_id as catbackId,d.catback_name as catbackName,d.catback_path as catbackPath,d.out_first_cate_id as rmsFirstCateId,d.out_second_cate_id as rmsSecondCateId,d.out_third_cate_id as rmsThirdCateId from gc_channel_product a left join gc_sku b on a.product_bn = b.sku_bn left join gc_spu c on b.spu_id = c.spu_id left join gc_background_category d on c.catback_id = d.catback_id";
		esMapping.setSql(sql);
		esMapping.setEtlCondition("where a.id>={0} and a.id<{1}");
		esMapping.setCommitBatch(3000);
		esMapping.setSchemaItem(SqlParser.parse(esMapping.getSql()));
		esMapping.setDirectJoin(true);
		config.setEsMapping(esMapping);
//		SqlParser.parse(config.getEsMapping().getSql());
		configMap.put("goods", config);
		esAdapter.getEsSyncService().sync(configMap.values(), dml);

	}
}