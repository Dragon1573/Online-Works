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

from re import search
from typing import List

from pandas import DataFrame
from pandas import Series
from pandas import concat
from pandas import read_csv
from tqdm import tqdm


def type_query_counts(data: DataFrame) -> DataFrame:
    """
    统计不同类型网页的访问次数

    :param data: 数据源
    :return: Pandas 表格
    """
    counts: Series = data['fullURLId'].value_counts()
    counts: DataFrame = counts.reset_index()
    counts.columns = ['fullURLId', 'counts']
    counts.set_index('fullURLId', inplace=True)
    return counts


def specified_type_query_counts(data: DataFrame, prefix: List[str]) -> DataFrame:
    """
    统计特定类型前缀的网页点击次数

    :param data: 数据集
    :param prefix: 类型前缀
    :return: 统计结果
    """
    counts = []
    for pattern in prefix:
        print('Scanning with prefix', pattern, '...')
        temp = 0
        for i in tqdm(data.loc[:, 'fullURLId'].apply(str)):
            temp += (search(pattern, i) is not None)
            pass
        counts.append(temp)
        print('Scanning Done!')
        pass
    frame = DataFrame({'typeID': prefix, 'counts': counts})
    frame.set_index('typeID', inplace=True)
    return frame


def user_query_counts(data: DataFrame) -> DataFrame:
    """
    统计用户点击次数的分布规律

    :param data: 数据集
    :return: 统计结果
    """
    frame: Series = data.loc[:, 'realIP'].value_counts()
    frame: DataFrame = frame.value_counts().reset_index()
    frame.columns = ['ip_queries', 'counts']
    frame.set_index('ip_queries', inplace=True)
    frame.loc[:, 'weight'] = frame.loc[:, 'counts'] / frame.shape[0]
    return frame


def url_click_counts(data: DataFrame) -> DataFrame:
    """
    网站点击次数统计

    :param data: 数据源
    :return: 统计结果
    """
    frame: Series = data.loc[:, 'fullURL'].value_counts()
    frame: DataFrame = frame.reset_index()
    frame.columns = ['fullURL', 'Counts']
    frame.set_index('fullURL', inplace=True)
    return frame


def url_click_rate(data: DataFrame) -> DataFrame:
    """
    网页被点击的次数分布规律

    :param data: 数据集
    :return: 统计结果
    """
    frame: Series = data.loc[:, 'fullURL'].value_counts()
    frame: Series = frame.value_counts()
    frame: DataFrame = frame.reset_index()
    frame.columns = ['url_queries', 'counts']
    frame.set_index('url_queries', inplace=True)
    frame.loc[:, 'weight'] = frame.loc[:, 'counts'] / frame.shape[0]
    return frame


def multiple_page_query_counts(data: DataFrame) -> DataFrame:
    """
    翻页网址访问数量统计

    :param data: 数据集
    :return: 统计结果
    """
    by_id: DataFrame = data[data['fullURL'].str.contains(r'_\d{0,2}.html')]
    by_params: DataFrame = data[data['fullURL'].str.contains(r'\&page=\d+.html')]
    by_page_number: DataFrame = data[data['fullURL'].str.contains(r'_\d_*\.html')]
    multiple_page_urls: DataFrame = concat([by_id, by_params, by_page_number], axis=0)
    multiple_page_urls: Series = multiple_page_urls['fullURL'].value_counts()
    multiple_page_urls: DataFrame = multiple_page_urls.reset_index()
    multiple_page_urls.columns = ['fullURL', 'counts']
    multiple_page_urls.loc[:, 'weight'] = multiple_page_urls.loc[:, 'counts'] / multiple_page_urls.shape[0]
    return multiple_page_urls


if __name__ == '__main__':
    print('数据载入中...')
    dataset: DataFrame = read_csv('../resources/all_gzdata.csv', encoding='GBK')
    print('数据载入完成！\n')

    print('不同类型的网页访问次数：', type_query_counts(data=dataset), '', sep='\n')
    print(
        '探索 101 和 1999 类型网页的访问次数：',
        specified_type_query_counts(data=dataset, prefix=['101', '1999']),
        '', sep='\n'
    )
    print('统计用户点击次数：', user_query_counts(data=dataset), '', sep='\n')

    print('网页点击分析：', url_click_counts(data=dataset), '', sep='\n')
    print('网页点击率分析：', url_click_rate(data=dataset), '', sep='\n')

    print('翻页网址各页面的访问次数：', multiple_page_query_counts(data=dataset), '', sep='\n')
