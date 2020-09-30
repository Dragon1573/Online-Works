'''
================================
训练样本特征拼接
================================
'''

offline_train['Discount_rate'] = offline_train['Discount_rate'].apply(get_discount)   # 特征2：折扣率
# offline_train['Discount_rate'].value_counts(dropna=False)
offline_train3 = pd.merge(offline_train, Coupon_popu, left_on='Coupon_id', right_on=Coupon_popu.index, how='left')   # 拼接特征3（优惠券流行度）
offline_train4 = pd.merge(offline_train3, Merchant_popu['Merchant_popu'], left_on='Merchant_id', right_on=Merchant_popu.index, how='left')   # 拼接特征4（商户流行度）
offline_train5 = pd.merge(offline_train4, number_received_coupon, left_on='User_id', right_on=number_received_coupon.index, how='left')    # 拼接特征5（用户领取的优惠券数量）
offline_train6 = pd.merge(offline_train5, number_used_coupon, left_on='User_id', right_on=number_used_coupon.index, how='left')    # 拼接特征6（用户消费的优惠券数量）

offline_train6_new = offline_train6.set_index(['User_id', 'Merchant_id'])
# 拼接特征7（用户在商家使用优惠券的次数）
# offline_train7 = pd.merge(offline_train6_new, e, left_on=offline_train6_new.index, right_on=e.index, how='left')
# offline_train7.fillna(0)
offline_train7 = pd.merge(offline_train6, user_merchant_used_coupon, left_on=['User_id', 'Merchant_id'], right_index=True, how='left')
offline_train8 = pd.merge(offline_train7, user_merchant_received_coupon, left_on=['User_id', 'Merchant_id'], right_index=True, how='left')
offline_train9 = pd.merge(offline_train8, user_merchant_cus, left_on=['User_id', 'Merchant_id'], right_index=True, how='left')
# offline_train9.fillna(method='bfill', inplace=True)
offline_train9.isnull().sum()
# offline_train9.to_csv('offline_train9.csv')








'''
================================
测试样本特征拼接
================================
'''
offline_test = pd.read_csv('data/ccf_offline_stage1_test_revised.csv')
offline_test['Discount_rate'] = offline_test['Discount_rate'].apply(get_discount)             # 特征2：折扣系数
offline_test3 = pd.merge(offline_test, Coupon_popu, left_on='Coupon_id', right_on= Coupon_popu.index, how='left')   # 拼接特征3（优惠券流行度）
offline_test4 = pd.merge(offline_test3, Merchant_popu['Merchant_popu'], left_on='Merchant_id', right_on=Merchant_popu.index, how='left')   # 拼接特征4（商户流行度）
offline_test5 = pd.merge(offline_test4, number_received_coupon, left_on='User_id', right_on=number_received_coupon.index, how='left')      # 拼接特征5
offline_test6 = pd.merge(offline_test5, number_used_coupon, left_on='User_id', right_on=number_used_coupon.index, how='left')              # 拼接特征6
offline_test7 = pd.merge(offline_test6, user_merchant_used_coupon, left_on=['User_id', 'Merchant_id'], right_index=True, how='left')       # 拼接特征7
offline_test8 = pd.merge(offline_test7, user_merchant_received_coupon, left_on=['User_id', 'Merchant_id'], right_index=True, how='left')   # 拼接特征8
offline_test9 = pd.merge(offline_test8, user_merchant_cus, left_on=['User_id', 'Merchant_id'], right_index=True, how='left')               # 拼接特征9
# offline_test9.to_csv('offline_test9.csv')
aaa = offline_test9[['user_merchant_used_coupon', 'user_merchant_received_coupon', 'user_merchant_cus']]
aaa.fillna(0, inplace=True)
offline_test9[['user_merchant_used_coupon', 'user_merchant_received_coupon', 'user_merchant_cus']] = aaa

offline_test9.isnull().sum()
offline_test9.fillna(method='ffill', inplace=True)
offline_test9.fillna(method='bfill', inplace=True)












