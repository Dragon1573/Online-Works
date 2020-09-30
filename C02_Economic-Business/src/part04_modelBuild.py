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

from typing import Tuple

from numpy import greater
from numpy import ndarray
from pandas import DataFrame
from pandas import Index
from pandas import Series
from pandas import read_pickle
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split


def get_married_slice(data: DataFrame) -> DataFrame:
    """
    获取『婚姻』相关的用户访问数据

    :param data: 数据集
    :return: 提取得到的数据分块
    """
    # 提取训练数据
    is_marriage_related: Series = data['fullURL'].str.contains('hunyin')
    sliced_data: DataFrame = data.loc[is_marriage_related, ['realIP', 'fullURL']]
    sliced_data.drop_duplicates(inplace=True)
    return sliced_data


def data_split(data: DataFrame, least_queries: int) -> Tuple[DataFrame, DataFrame]:
    """
    数据集划分

    :param data: 数据集
    :param least_queries: 最小访问次数，仅提取浏览网页次数超过此阈值的用户 IP 地址
    :return: 划分结果，返回一个二元元组，每一个元素都是 DataFrame
    """
    ip_query_counts: Series = data['realIP'].value_counts()
    # 浏览次数过小的用户在训练集中没有足够的数据量判断其喜好
    # 在测试集中也没有足够的数据量去检查模型的正确性
    # 集中在此过滤以降低模型构建难度
    validated_ip: Series = ip_query_counts[greater(ip_query_counts, least_queries)]
    ip_train: Series
    ip_test: Series
    ip_train, ip_test = train_test_split(validated_ip, test_size=0.2, random_state=0X6A24261B)
    train_indexer: list = [ip in ip_train for ip in data['realIP']]
    test_indexer: list = [ip in ip_test for ip in data['realIP']]
    train_data: DataFrame = data[train_indexer]
    test_data: DataFrame = data[test_indexer]
    return train_data, test_data


def get_ip_url_relationships(data: DataFrame) -> DataFrame:
    """
    获取『用户-物品矩阵』

    :param data: 数据集
    :return: pandas.DataFrame 矩阵
    """
    ip_array: ndarray = data['realIP'].unique()
    url_array: ndarray = data['fullURL'].unique()
    matrix: DataFrame = DataFrame(0, index=Index(ip_array, dtype=str), columns=Index(url_array, dtype=str))
    for row in data.itertuples(False, name='Row'):
        matrix.loc[row.realIP, row.fullURL] = 1
        pass
    return matrix


def get_url_similarities(user_item_matrix: DataFrame) -> DataFrame:
    """
    构建物品（余弦）相似度矩阵

    :param user_item_matrix: 用户-物品关联矩阵
    :return: pandas.DataFrame 矩阵
    """
    url_series: Index = user_item_matrix.columns
    similarity_matrix: ndarray = cosine_similarity(X=user_item_matrix.transpose())
    similarity_matrix: DataFrame = DataFrame(similarity_matrix, columns=url_series, index=url_series)
    for index in range(similarity_matrix.shape[0]):
        # 余弦相似度矩阵中的对角线全为 1 ，
        # 后续使用 argmax() 获取最大元素索引值时会取到自己身上
        similarity_matrix.iloc[index, index] = 0
        pass
    return similarity_matrix


if __name__ == '__main__':
    print('载入已清洗的数据...')
    dataset: DataFrame = read_pickle('../resources/cleaned_data.pkl')
    print('载入完成！\n')

    print('提取婚姻数据集...')
    dataset: DataFrame = get_married_slice(data=dataset)
    print('婚姻数据集提取完成！\n')

    print('划分数据集...')
    train_set, test_set = data_split(data=dataset, least_queries=2)
    print('数据集划分完成！\n')

    print('构建训练集的『用户-物品矩阵』...')
    ip_url_related_matrix: DataFrame = get_ip_url_relationships(data=train_set)
    print('构建完成！\n')

    print('构建训练集的『物品相似度矩阵』...')
    url_similarity_matrix: DataFrame = get_url_similarities(user_item_matrix=ip_url_related_matrix)
    print('构建完成！\n')

    print('数据另存中...')
    train_set.to_pickle(path='../resources/data_train.pkl')
    test_set.to_pickle(path='../resources/data_test.pkl')
    url_similarity_matrix.to_pickle(path='../resources/train_similarities.pkl')
    print('模型与数据集保存成功！')
    pass
