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
from numpy import ndarray
from numpy import not_equal
from pandas import DataFrame
from pandas import RangeIndex
from pandas import read_pickle
from typing import Dict
from typing import List


def get_user_query_map(_data: DataFrame) -> Dict[str, List[str]]:
    """
    获取用户访问映射表

    :param _data: 测试集
    :return: 映射字典
    """
    # 提取唯一的测试用户列表
    test_ip: ndarray = _data['realIP'].unique()
    test_ip_dict: Dict[str, List[str]] = {
        # 遍历用户列表中的每一个 IP 地址，获得 IP 访问的所有 URL 链接
        ip: list(_data.loc[_data['realIP'] == ip, 'fullURL']) for ip in test_ip
    }
    return test_ip_dict


def nomination_matrix(_url_train: ndarray, _data_test: DataFrame, _similarities: DataFrame) -> DataFrame:
    """
    获取推荐矩阵

    :param _url_train: 训练集
    :param _data_test: 测试集
    :param _similarities: 相似度矩阵
    :return: 推荐矩阵
    """
    matrix: DataFrame = DataFrame(
        data=None, index=RangeIndex(_data_test.shape[0]),
        columns=['realIP', 'fullURL', 'nominationURL']
    )
    matrix.loc[:, 'realIP'] = _data_test.loc[:, 'realIP']
    matrix.loc[:, 'fullURL'] = _data_test.loc[:, 'fullURL']
    for index in matrix.index:
        if matrix.loc[index, 'fullURL'] in _url_train:
            matrix.loc[index, 'nominationURL'] = _url_train[
                _similarities.loc[matrix.loc[index, 'fullURL'], :].argmax()
            ]
            pass
        pass
    return matrix


def performance(target: DataFrame, judgement: Dict[str, List[str]], train_set: DataFrame) -> None:
    """
    获取模型评估指标

    :param target: 推荐矩阵
    :param judgement: 测试用户访问映射表
    :param train_set: 训练集
    :return: 推荐正确率
    """
    for index in target.index:
        if target.loc[index, 'fullURL'] in train_set['fullURL'].unique():
            target.loc[index, 'is_correct'] = (
                    target.loc[index, 'nominationURL'] in judgement[target.loc[index, 'realIP']]
            )
            pass
        pass
    bingo: float = target[equal(target['is_correct'], True)].shape[0]
    print(f"模型的准确率（Accuracy）为：{bingo / target.shape[0]}")
    print(
        '模型的精确率（Precision Rate）为：',
        bingo / (target.shape[0] - sum(target['is_correct'].notna()))
    )


if __name__ == '__main__':
    print('载入数据...')
    train: DataFrame = read_pickle(filepath_or_buffer='../resources/data_train.pkl')
    train.reset_index(drop=True, inplace=True)
    test: DataFrame = read_pickle(filepath_or_buffer='../resources/data_test.pkl')
    test.reset_index(drop=True, inplace=True)
    similarity_matrix: DataFrame = read_pickle(filepath_or_buffer='../resources/train_similarities.pkl')
    print('数据载入完成！\n')

    print('构建测试用户访问字典...')
    test_query_dictionary: Dict[str, List[str]] = get_user_query_map(_data=test)
    print('构建完成！\n')

    print('构建推荐矩阵...')
    url_array: ndarray = train['fullURL'].unique()
    nominations: DataFrame = nomination_matrix(_url_train=url_array, _data_test=test, _similarities=similarity_matrix)
    print('构建完成！\n')

    performance(target=nominations, judgement=test_query_dictionary, train_set=train)
    pass
