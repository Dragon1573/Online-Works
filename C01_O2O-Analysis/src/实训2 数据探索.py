# 数据探索
indexOne = offline_train['Discount_rate'].astype(str).apply(lambda x: re.findall('\d+:\d+', x) != [])   # 满减优惠形式的索引
indexTwo = offline_train['Discount_rate'].astype(str).apply(lambda x: re.findall('\d+\.\d+', x) != [])  # 折扣率优惠形式的索引
dfOne = offline_train.loc[indexOne,:]   # 取出满减优惠形式的数据
dfTwo = offline_train.loc[indexTwo,:]   # 取出折扣率优惠形式的数据
len(dfOne)      # 满减优惠形式的数据的数量
len(dfTwo)      # 折扣率优惠形式的数据的数量
numberOne = sum(dfOne['label'] == 1)   # 在满减优惠形式的数据中，15天内优惠券被使用的数目
numberTwo = sum(dfOne['label'] == 0)   # 在满减优惠形式的数据中，15天内优惠券未被使用的数目
numberThree = sum(dfTwo['label'] == 1)   # 在折扣率优惠形式的数据中，15天内优惠券被使用的数目
numberFour = sum(dfTwo['label'] == 0)   # 在折扣率优惠形式的数据中，15天内优惠券未被使用的数目

plt.figure(figsize=(6,3))           # 创建画布
plt.rcParams['font.sans-serif'] = 'Simhei'          # 设置可现实中文字体
plt.subplot(1,2,1)                  # 创建子图
plt.pie([numberOne,numberTwo],autopct='%.1f%%')     # 画出两种优惠形式中优惠券的使用情况饼图
plt.legend(['优惠券15天内被使用','优惠券15天内未使用'],fontsize=15)     # 添加图例
plt.title('满减优惠形式的优惠券使用情况',fontsize=25)         # 添加标题

plt.subplot(1,2,2)          # 创建子图
plt.pie([numberThree,numberFour],autopct='%.1f%%')      # 画出两种优惠形式中优惠券的使用情况饼图
plt.legend(['优惠券15天内被使用','优惠券15天内未使用'],fontsize=15)     # 添加图例
plt.title('折扣率优惠形式的优惠券使用情况',fontsize=25)        # 添加标题
