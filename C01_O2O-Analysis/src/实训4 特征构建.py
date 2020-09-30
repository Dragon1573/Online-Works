# 构建特征3 & 4：优惠券受欢迎程度（被使用优惠券/优惠券总数）&各商户所发放优惠券的受欢迎程度（被使用优惠券/发放优惠券总数）
Coupon_popu = offline_train[['Coupon_id', 'label']].groupby('Coupon_id').agg(lambda x: sum(x == 1)/len(x))  # 特征3：优惠券流行度
Coupon_popu.columns = ['Coupon_popu']
# Coupon_popu.to_csv('Coupon_popu.csv')
Merchant_popu = offline_train[['Merchant_id', 'Coupon_id', 'label']].groupby('Merchant_id').\
    agg({'label': lambda x: sum(x == 1), 'Coupon_id': lambda x: sum(x.notnull())})                 # 特征4：商户流行度
Merchant_popu['Merchant_popu'] = Merchant_popu['label']/Merchant_popu['Coupon_id']
# Merchant_popu.to_csv('Merchant_popu.csv')

# 特征5：用户领取的优惠券数量
number_received_coupon = offline_train.groupby('User_id').agg({'Coupon_id': lambda x: sum(x.notnull())})
number_received_coupon.columns = ['number_received_coupon']

# 特征6：用户消费过的优惠券数量
number_used_coupon = offline_train.groupby('User_id').agg({'label': lambda x: sum(x == 1)})
number_used_coupon.columns = ['number_used_coupon']

# 特征7：用户在商家使用优惠券的次数
user_merchant_used_coupon = offline_train.groupby(['User_id', 'Merchant_id']).agg({'label': lambda x: sum(x == 1)})
user_merchant_used_coupon.columns = ['user_merchant_used_coupon']

# 特征8：用户在商家领取的优惠券数
user_merchant_received_coupon = offline_train.groupby(['User_id', 'Merchant_id']).agg({'Coupon_id': lambda x: sum(x.notnull())})
user_merchant_received_coupon.columns = ['user_merchant_received_coupon']

# 特征9：用户在商家消费的次数
user_merchant_cus = offline_train.groupby(['User_id', 'Merchant_id']).agg({'Date': lambda x: sum(x.notnull())})
user_merchant_cus.columns = ['user_merchant_cus']
