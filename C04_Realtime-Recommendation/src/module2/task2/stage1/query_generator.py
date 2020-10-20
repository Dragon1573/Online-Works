# -*- coding:UTF-8 -*-
from random import choice
from random import randint
from random import sample
from string import ascii_letters
from string import ascii_lowercase
from string import capwords
from string import digits
from sys import argv
from time import time
from tqdm import tqdm


def get_random(instr, length):
    """
    产生特定长度的随机字符串

    :param instr: 随机来源
    :param length: 随机长度
    """
    # 随机提取一定数量的基础字符串
    res = sample(instr, length)
    # 将这些字符串拼接在一起
    result = ''.join(res)
    return result


# 放置已生成的行索引编号
row_key_tmp_list = []


def get_random_row_key():
    """
    随机产生行索引编号

    :return:
    """
    while True:
        # 获取00~99的两位数字，包含00与99
        num = randint(00, 99)
        # 获取当前10位的时间戳
        timestamp = int(time())
        # 产生合法行索引号
        pre_row_key = str(num).zfill(2) + str(timestamp)
        # 如果索引号不存在，则将其添加到索引缓存中
        if pre_row_key not in row_key_tmp_list:
            row_key_tmp_list.append(pre_row_key)
            break
            pass
        pass
    return pre_row_key


def get_random_name(length):
    """
    创建用户名

    :param length: 用户名的长度
    :return: 随机产生的用户名
    """
    return capwords(get_random(ascii_lowercase, length))


def get_random_age():
    """
    获取随机年龄

    :return: 年龄（18～60岁）
    """
    return str(randint(18, 60))


def get_random_sex():
    """
    获取随机性别

    :return: 性别
    """
    return choice(("woman", "man"))


def get_random_goods_no():
    """
    随机获取商品编号

    :return: 商品编号（共计12个商品）
    """
    return choice((
        "220902", "430031", "550012", "650012", "532120", "230121",
        "250983", "480071", "580016", "950013", "152121", "230121"
    ))


def get_random_goods_price():
    """
    随机构建商品价格

    :return: 价格（字符型式的2位浮点小数）
    """
    # 元
    price_int = randint(1, 999)
    # 角与分
    price_decimal = randint(1, 99)
    return str(price_int) + "." + str(price_decimal)


def get_random_store_id():
    """
    获取随机商户编号

    :return: 商户编号（共计12个商户）
    """
    return choice([
        "313012", "313013", "313014", "313015", "313016", "313017",
        "313018", "313019", "313020", "313021", "313022", "313023"
    ])


def get_random_goods_type():
    """
    随机获取用户行为

    :return: 枚举字符串（点击、购买、加单、收藏、浏览）
    """
    return choice(["pv", "buy", "cart", "fav", "scan"])


def get_random_tel():
    """
    随机产生手机号码

    :return: 标准手机号码
    """
    # 随机提取手机号段
    isp_prefix = choice([
        "130", "131", "132", "133", "134", "135",
        "136", "137", "138", "139", "147", "150",
        "151", "152", "153", "155", "156", "157",
        "158", "159", "186", "187", "188"
    ])
    # 随机生成8位号码
    return isp_prefix + ''.join(sample(digits, 4))


def get_random_email(length):
    """
    随机获取邮箱地址

    :param length: 用户名称的长度
    :return: 合法邮箱
    """
    return "@".join((
        get_random(ascii_letters, length),
        choice(["163.com", "126.com", "qq.com", "gmail.com", "huawei.com"])
    ))


def get_random_buy_time():
    """
    随机产生商品交易日期

    :return: 日期（2019年8月1日～7日）
    """
    return choice(["2019-08-01", "2019-08-02", "2019-08-03", "2019-08-04", "2019-08-05", "2019-08-06", "2019-08-07"])


def get_random_record():
    """
    随机生成一条记录

    :return: 用户日志记录
    """
    return ",".join((
        get_random_row_key(), get_random_name(5), get_random_age(), get_random_sex(), get_random_goods_no(),
        get_random_goods_price(), get_random_store_id(), get_random_goods_type(), get_random_tel(),
        get_random_email(10), get_random_buy_time()
    ))


if __name__ == "__main__":
    """ 将生成的用户日志写入指定文件中 """
    if len(argv) != 3:
        raise SyntaxError("用法：python query_generator.py <目标文件> <记录数量>")
        pass
    else:
        with open(argv[1], "w") as file:
            for _ in range(int(argv[2])):
                record = get_random_record()
                file.write(record)
                file.write('\n')
                pass
            pass
        pass
    pass
