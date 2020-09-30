# 特征1： 'Distance字段的缺失值填充'；

# b= offline_train[['label', 'Distance']].groupby('label')
# for index, data in b:
#     print(index)
#     print(data)

# Distance列的缺失值处理
offline_train['Distance'].isnull().sum()        # 缺失值总数
# Out[23]: 106003

offline_train[['label', 'Distance']].groupby('label').agg(lambda x: sum(x.isnull()))   # 三种标签的缺失值个数
# label     Distance
# -1          0.0
#  0      98668.0
# 1	7335.0
# 对Distance字段进行缺失值填充
offline_train['Distance'].fillna(method='ffill', inplace=True)
offline_train['Distance'].fillna(method='bfill', inplace=True)
offline_train['Distance'].value_counts()

# 特征2：优惠券的折扣率转换函数的构建
def get_discount(x):
    # 将满减优惠改写成折扣率形式：（300：30 等价于 0.9折）
    try:
        if re.findall('\d+:\d+', x) != []:
            a = x.split(':')
            return 1 - float(a[1])/float(a[0])
        else:
            return x
    except:
        return 1

