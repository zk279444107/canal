package com.alibaba.otter.canal.client.adapter.es.test.sync;

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

		String database = dml.getDatabase();
		String table = dml.getTable();
		Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

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
	
	/**
	 * 单表插入
	 */
	@Test
	public void test02() {
//		String ss = "{\"data\":[{\"id\":24547,\"channel\":\"JD\",\"sku_bn\":\"105824727\",\"sku_name\":\"贝涛8寸玩偶\",\"safety_stock\":null,\"out_sku_id\":2021770360,\"out_shop_category_id\":4638359,\"out_shop_category_name\":\"日用家纺\",\"out_first_category_id\":21929,\"out_first_category_name\":\"玩具\",\"out_second_category_id\":21930,\"out_second_category_name\":\"毛绒玩具\",\"out_third_category_id\":21938,\"out_third_category_name\":\"其他毛绒\",\"out_brand_id\":35247,\"out_brand_name\":\"其他品牌\",\"serial_no\":103405,\"remark\":\"\",\"gmt_create\":1547626872147,\"gmt_modified\":1557476580018,\"available\":\"A\"}],\"database\":\"bbg_plat_goods\",\"destination\":\"goods\",\"es\":1557476580000,\"groupId\":null,\"isDdl\":false,\"old\":[{\"serial_no\":103403,\"gmt_modified\":1557476280016}],\"pkNames\":[\"id\"],\"sql\":\"\",\"table\":\"gc_channel_sku\",\"ts\":1557505390327,\"type\":\"UPDATE\"}";
//		String ss = "{\"data\":[{\"id\":757118,\"channel\":\"BTGO\",\"md_code\":\"120036\",\"product_bn\":\"107263775\",\"prompt_price\":3996,\"prompt_code\":\"03\",\"begin_time\":1548086400000,\"end_time\":1549814399000,\"bill_no\":\"2019012137005120037\",\"prompt_type\":\"3\",\"out_prompt_id\":null,\"serial_no\":1299707,\"remark\":\"\",\"gmt_create\":1548086530016,\"gmt_modified\":1557653134271}],\"database\":\"bbg_plat_goods\",\"destination\":\"goods\",\"es\":1557653134000,\"groupId\":null,\"isDdl\":false,\"old\":[{\"prompt_price\":3995,\"gmt_modified\":1557597958834}],\"pkNames\":[\"id\"],\"sql\":\"\",\"table\":\"gc_channel_prompt_product\",\"ts\":1557681944652,\"type\":\"UPDATE\"}";
		String ss = "{\"data\":[{\"id\":656231,\"channel\":\"BTGO\",\"md_code\":\"120036\",\"product_bn\":\"107263775\",\"normal_price\":4992,\"synchronizable\":\"A\",\"available\":\"A\",\"available_type\":\"A\",\"serial_no\":2690985,\"remark\":\"\",\"gmt_create\":1547711000015,\"gmt_modified\":1557676085674}],\"database\":\"bbg_plat_goods\",\"destination\":\"goods\",\"es\":1557676085000,\"groupId\":null,\"isDdl\":false,\"old\":[{\"normal_price\":4991,\"gmt_modified\":1557676058951}],\"pkNames\":[\"id\"],\"sql\":\"\",\"table\":\"gc_channel_product\",\"ts\":1557704896538,\"type\":\"UPDATE\"}";
		Dml dml = JSON.parseObject(ss, Dml.class);

		String database = dml.getDatabase();
		String table = dml.getTable();
		Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

		Map<String, ESSyncConfig> configMap = ESSyncConfigLoader.load(null);
		
		ESSyncConfig config = new ESSyncConfig();
		config.setDataSourceKey("goodsDS");
		config.setDestination("goods");
		config.setGroupId("g2");
		
		ESMapping esMapping = new ESMapping();
		esMapping.set_index("channel_goods");
		esMapping.set_type("_doc");
		esMapping.set_id("_id");
		esMapping.setUpsert(true);
//		esMapping.setSql("select concat(a.md_code,'_',a.product_bn,'_',a.channel) as _id,a.id as channel_product_id,a.product_bn,a.md_code,a.channel,a.normal_price,GROUP_CONCAT(b.id order by b.id desc separator ';') AS prompt_ids,GROUP_CONCAT(b.prompt_price order by b.id desc separator ';') AS prompt_prices,GROUP_CONCAT(b.prompt_code order by b.id desc separator ';') AS prompt_codes,GROUP_CONCAT(b.bill_no order by b.id desc separator ';') AS prompt_bill_nos,c.id as channel_sku_id,c.sku_name from gc_channel_product a left join gc_channel_prompt_product b on a.md_code = b.md_code and a.channel = b.channel and a.product_bn = b.product_bn left join gc_channel_sku c on a.product_bn = c.sku_bn and a.channel = c.channel");
		String sql = "select concat( a.md_code, '_', a.product_bn, '_', a.channel ) as _id,(select group_concat( e.id order by e.id desc separator ';' ) as prompt_ids from gc_channel_prompt_product e where e.channel = a.channel and e.md_code = a.md_code and e.product_bn = a.product_bn) as prompt_ids,(select group_concat( e.prompt_price order by e.id desc separator ';' ) as prompt_prices from gc_channel_prompt_product e where e.channel = a.channel and e.md_code = a.md_code and e.product_bn = a.product_bn) as prompt_prices from gc_channel_product a";
		esMapping.setSql(sql);
		esMapping.setEtlCondition("where a.id>={0} and a.id<{1}");
		esMapping.setCommitBatch(3000);
		esMapping.setSchemaItem(SqlParser.parse(esMapping.getSql()));
		config.setEsMapping(esMapping);
//		SqlParser.parse(config.getEsMapping().getSql());
		configMap.put("goods", config);
		esAdapter.getEsSyncService().sync(configMap.values(), dml);

	}
}
