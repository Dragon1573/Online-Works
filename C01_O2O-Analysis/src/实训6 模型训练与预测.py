# sum(offline_train9['label'] == 1)
# sum(offline_train9['label'] == 0)
# 模型训练函数
def train(model=GradientBoostingClassifier(n_estimators=100, max_depth=5), data=None,
          features=['Discount_rate', 'Distance'], label='label', pos_number=64395, neg_number=988887):
    a = data[data[label] == 1].sample(pos_number)    # 抽取正样本
    b = data[data[label] == 0].sample(neg_number)    # 抽取负样本
    c = a.append(b)
    X = c.loc[:, features]    # 样本特征
    y = c.loc[:, label]       # 样本目标值
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, stratify=y)   # 样本集划分
    model.fit(X_tr, y_tr)     # 模型训练
    pre = model.predict_proba(X_te)        # 输出预测概率
    auc = roc_auc_score(y_te, pre[:, 1])   # 计算AUC值
    pre = model.predict(X_te)
    con_mat = confusion_matrix(y_te, pre)
    return model, auc, con_mat



# 模型训练
model, auc, con_mat = train(
    features=['Discount_rate', 'Distance',
              'Merchant_popu', 'number_received_coupon',
              'number_used_coupon', 'user_merchant_used_coupon',
              'user_merchant_received_coupon', 'user_merchant_cus'],
data=offline_train9)

# 模型预测
features_test = ['Discount_rate', 'Distance',
                  'Merchant_popu', 'number_received_coupon',
                  'number_used_coupon', 'user_merchant_used_coupon',
                  'usermerchant_received_coupon', 'user_merchant_cus']

pre = model.predict_proba(offline_test9[features_test])[:, 1]
# sum(pre > 0.5)
offline_test['pre'] = pre
offline_test[['User_id', 'Coupon_id', 'Date_received', 'pre']].to_csv('submission0911_3.csv', index=None, header=None)  # 将预测结果写出
