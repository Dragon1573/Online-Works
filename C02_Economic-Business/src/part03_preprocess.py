#  Copyright (C) 2020 Dragon1573
#
#      This program is free software: you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation, either version 3 of the License, or
#      (at your option) any later version.
#
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#
#      You should have received a copy of the GNU General Public License
#      along with this program.  If not, see <https://www.gnu.org/licenses/>.

from numpy import equal
from numpy import logical_not
from pandas import DataFrame
from pandas import Index
from pandas import read_csv


def dirty_explore(data: DataFrame) -> None:
    print(
        '非 HTML 页面访问次数为：',
        data[equal(data['fullURL'].str.contains(r'\.html'), False)].shape[0]
    )
    print(
        '所有用户的咨询发布量为：',
        data[equal(data['pageTitle'].str.contains('咨询发布成功'), True)].shape[0]
    )
    print(
        '中间页面访问次数为：',
        data[equal(data['fullURL'].str.contains('midques_'), True)].shape[0]
    )
    print(
        '动态链接访问量为：',
        data[equal(data['fullURL'].str.contains(r'\?'), True)].shape[0]
    )
    print(
        '律师行为相关访问记录数量为：',
        data[equal(data['pageTitle'].str.contains(r'法律快车-律师助手'), True)].shape[0]
    )
    print(
        '跨站访问量为：',
        data[equal(data['fullURL'].str.contains(r'lawtime'), False)].shape[0]
    )
    pass


def preprocesses(data: DataFrame) -> DataFrame:
    """
    数据集预处理

    :param data: 未经处理的原始数据集
    :return: 经过预处理的数据集
    """
    data: DataFrame = data.loc[data['fullURL'].str.contains('.html', regex=False), :]
    data: DataFrame = data.loc[logical_not(data['pageTitle'].str.contains('咨询发布成功', regex=False)), :]
    data: DataFrame = data.loc[logical_not(data['fullURL'].str.contains('midques_', regex=False)), :]
    is_dynamic: Index = data['fullURL'].str.contains('?', regex=False)
    data.loc[is_dynamic, 'fullURL'] = data.loc[is_dynamic, 'fullURL'].str.replace(r'\?(.*)', '')
    data: DataFrame = data.loc[logical_not(data['pageTitle'].str.contains('法律快车-律师助手')), :]
    data: DataFrame = data[data['fullURL'].str.contains('lawtime')]
    data.drop_duplicates(inplace=True)
    data.loc[:, 'fullURL'] = data.loc[:, 'fullURL'].str.replace(r'_\d{1,2}.html', '.html')
    data.loc[:, 'fullURL'] = data.loc[:, 'fullURL'].str.replace(r'_\d+_p\d+.html', '.html')
    data.drop_duplicates(inplace=True)
    return data


if __name__ == '__main__':
    print('数据载入中...')
    dataset: DataFrame = read_csv('../resources/all_gzdata.csv', encoding="GBK", dtype=str)
    print('数据载入完成！\n')

    print('脏数据统计中...')
    dirty_explore(data=dataset)
    print('脏数据统计完毕！\n')

    # 数据预处理
    print('数据清洗中...')
    dataset: DataFrame = preprocesses(dataset)
    print('清洗完成！\n')

    # 将清洗结果以 CSV 纯文本形式另存
    print('将清洗结果另存为 Pickle 文件...')
    dataset.to_pickle('../resources/cleaned_data.pkl')
    print('存储成功！\n')
