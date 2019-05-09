package com.alibaba.otter.canal.client.adapter.es.test.sync;

import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.es.ESAdapter;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
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

		esAdapter.getEsSyncService().sync(esSyncConfigs.values(), dml);

		GetResponse response = esAdapter.getTransportClient().prepareGet("mytest_user", "_doc", "1").get();
		Assert.assertEquals("Eric", response.getSource().get("_name"));
	}
}
