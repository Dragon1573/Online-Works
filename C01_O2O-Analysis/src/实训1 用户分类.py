import pandas

offline_train = pandas.read_csv('ccf_offline_stage1_train.csv',
                                parse_dates=['Date_received', 'Date'])

# 正负样本划分
offline_train['label'] = -1  # -1 代表普通用户，1代表正样本， 0代表负样本
index1 = (offline_train['Date'] - offline_train['Date_received']).apply(
    lambda x: x.days <= 15)  # 15天内优惠券被使用的数据的索引
index2 = (offline_train['Date'] - offline_train['Date_received']).apply(
    lambda x: x.days > 15)  # 15天内优惠券未被使用的数据的索引
index3 = offline_train['Coupon_id'].notnull() & offline_train[
    'Date'].isnull()  # 领了优惠券却未使用
offline_train.loc[index1, 'label'] = 1  # 正样本1：date - date_re < 15
offline_train.loc[(index2 | index3), 'label'] = 0
# 负样本0: date - date_re > 15 | (Coupon_id not null & date is null)
offline_train['label'].value_counts()
