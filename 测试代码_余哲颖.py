import pandas as pd
from datetime import datetime, timedelta

test_rawdata = pd.read_csv('test.csv')
futures_rawdata = pd.read_csv('futures_description.csv')
# 合并
rawdata = test_rawdata.merge(futures_rawdata, how='inner', left_on='CONTRACT', right_on='symbol').sort_values(by='DATE',
                                                                                                              ascending=False)

code_list = list(rawdata.code.unique())  # 所有需要遍历的品种


def get_info(code):
    '''1. 获得每个品种的主力和次力合约'''

    def get_dom(code):
        '''对每个期货品种，计算主力合约'''
        part_data = rawdata.loc[rawdata.code == code]
        starttime, endtime = part_data.sort_values(by='DATE').iloc[0, 3], part_data.sort_values(by='DATE').iloc[
            -1, 3]  # 这个期货品种的起始时间和结束时间

        # 按照开始时间和结束时间，创建时间列表。
        day_gap = (datetime.strptime(endtime, "%Y-%m-%d") - datetime.strptime(starttime, "%Y-%m-%d")).days
        day_list = [(datetime.strptime(endtime, "%Y-%m-%d") - timedelta(days=i)).strftime("%Y-%m-%d") for i in
                    range(0, day_gap)]

        # 创建新的dataframe 填充时间列表和期货种类
        part_df = pd.DataFrame(columns=['date', 'code'])
        part_df['date'] = day_list
        part_df['code'] = code

        # 接下来得任务是填充part_df 表格，找到每个时间的主力合约次主力合约
        def get_max_openint(x):
            '''输出持仓量最大的合约'''
            contract = part_data.loc[part_data.DATE == x].sort_values(by='OPENINT', ascending=True)
            if not contract.empty:
                return contract.iloc[0, 2]

        part_df['max_openint'] = part_df.date.apply(lambda x: get_max_openint(x))

        # 可以看到有许多空值，现在暂时对空值处理。需要在最大持仓量的合约中找到连续三天持仓的合约， 使其在第4个交易日成为主力合约
        max_openint_list = list(part_df.max_openint)

        for i in range(len(max_openint_list), 2, -1):
            j = i - 3
            a_list = max_openint_list[j:i]
            if a_list[0] == a_list[1] == a_list[2]:
                part_df.loc[part_df.index == j - 1, 'dom_contract'] = a_list[2]

        # 如果某主力合约在到期日仍然为持仓量最大，在剩余该品种合约中在最近3个交易日内，累计持仓量最大的合约作为下一个交易日的主力合约
        aa = part_data[['CONTRACT', 'maturity_date']].drop_duplicates()  # 记录到期时间
        part_df2 = part_df.merge(aa, how='left', left_on='dom_contract', right_on='CONTRACT')
        change_dom = part_df2.loc[
            (part_df2.maturity_date.notnull()) & (part_df2.date == part_df2.maturity_date)]  # 需要转化主力合约的表格

        def get_max_openint2(x, feature):
            '''3天内累计持仓量最大的合约'''
            starttime, endtime = (datetime.strptime(x.date, "%Y-%m-%d") - timedelta(days=2)).strftime("%Y-%m-%d"), (
                        datetime.strptime(x.date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            # 计算时间内累计最大持仓量的合约
            sum_3day = part_data.loc[(part_data.CONTRACT != x[feature]) & (part_data.DATE < endtime) & (
                        part_data.DATE >= starttime)].groupby(by='CONTRACT')['OPENINT'].sum().to_frame().reset_index()
            # 输出合约名字
            if not sum_3day.empty:
                return sum_3day.sort_values(by='OPENINT', ascending=False).iloc[0, 0]

        if not change_dom.empty:  # 有可能没有需要转化主力的合约
            # 更新需要转化主力合约的表格
            change_dom['dom_contract'] = change_dom.apply(lambda x: get_max_openint2(x, 'max_openint'), axis=1)

            # 更新part_df应该输出的表格中的主力合约
            part_df.loc[part_df.index.isin(list(change_dom.index + 1)), 'dom_contract'] = change_dom.dom_contract

        # 输出主力合约表格
        output_part_df = part_df.loc[part_df.dom_contract.notnull(), ['date', 'code', 'dom_contract']].reset_index(
            drop=True)

        return output_part_df

    def get_subdom(code):
        '''输出次主力合约'''
        output_part_df = get_dom(code)
        part_data = rawdata.loc[rawdata.code == code]
        # 首先找到转换主力合约的次主力合约
        switchdom_df = output_part_df.drop_duplicates(subset='dom_contract', keep="last")

        def get_max_openint3(x, feature):
            '''发生前三天内累计持仓量最大的合约， 和get_max_openint2 区别是时间不同'''
            starttime, endtime = (datetime.strptime(x.date, "%Y-%m-%d") - timedelta(days=3)).strftime(
                "%Y-%m-%d"), x.date
            # 计算时间内累计最大持仓量的合约
            sum_3day = part_data.loc[(part_data.CONTRACT != x[feature]) & (part_data.DATE < endtime) & (
                        part_data.DATE >= starttime)].groupby(by='CONTRACT')['OPENINT'].sum().to_frame().reset_index()
            # 输出合约名字
            if not sum_3day.empty:
                return sum_3day.sort_values(by='OPENINT', ascending=False).iloc[0, 0]

                # 添加字段 次主力合约

        switchdom_df['subdom_contract'] = switchdom_df.apply(lambda x: get_max_openint3(x, 'dom_contract'), axis=1)

        # 更新到 output_part_df
        output_part_df2 = switchdom_df.merge(output_part_df.dom_contract, how='left', on='dom_contract')

        return output_part_df2[['date', 'code', 'subdom_contract']]

    # 输出合约所有12columns信息
    def contract_factors(code):
        '''获得该期货的所有信息，包括主力合约复合因子等
        1. 主力合约
        2. 次主力合约
        分开计算'''
        part_data = rawdata.loc[rawdata.code == code]
        # 主力合约
        part_dom = get_dom(code)  # 获得主力合约
        # 从rawdata获得信息并且合并
        part_actorclose_dom = part_dom.merge(
            rawdata[['DATE', 'CONTRACT', 'OPENINT', 'SETTLEMENT', 'symbol', 'close', ]], how='left',
            left_on=['date', 'dom_contract'], right_on=['DATE', 'CONTRACT'])

        def get_pre_close(x, feature):
            # 上一个交易日日期
            pre_date = (datetime.strptime(x.date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            part_data1 = part_data.loc[(part_data.CONTRACT == x[feature]) & (part_data.DATE == pre_date)]

            if not part_data1.empty:
                return part_data1.close.values[0]

        def get_pre_settlement(x, feature):
            # 上一个交易日日期
            pre_date = (datetime.strptime(x.date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            part_data1 = part_data.loc[(part_data.CONTRACT == x[feature]) & (part_data.DATE == pre_date)]

            if not part_data1.empty:
                return part_data1.SETTLEMENT.values[0]

        def get_next_symbol(x, feature):
            # 下一个交易日期
            next_date = (datetime.strptime(x.date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            part_data2 = part_data.loc[(part_data.CONTRACT == x[feature]) & (part_data.DATE == next_date)]

            if not part_data2.empty:
                return part_data2.symbol.values[0]

        # 新增三个字段
        part_actorclose_dom['pre_close'] = part_actorclose_dom.apply(lambda x: get_pre_close(x, 'dom_contract'), axis=1)
        part_actorclose_dom['pre_settlement'] = part_actorclose_dom.apply(
            lambda x: get_pre_settlement(x, 'dom_contract'), axis=1)
        part_actorclose_dom['next_symbol'] = part_actorclose_dom.apply(lambda x: get_next_symbol(x, 'dom_contract'),
                                                                       axis=1)

        def get_factors(df, feature):
            '''计算复权因子factor_close & factor_settlement， 计算方式一致'''

            # 初始复权因子 = 1
            f_actorclose = 1
            f_actorsettlement = 1
            contract_i = df.loc[df.index == len(list(df.index)) - 1][feature].values[0]
            for i in range(len(list(df.index)) - 1, 0, -1):
                j = i - 1
                contract_j = df.loc[df.index == j][feature].values[0]
                # 首先更新表格
                df.loc[df.index == i, 'factor_close'] = f_actorclose
                df.loc[df.index == i, 'factor_settlement'] = f_actorsettlement

                # 如果下一个交易日主力合约不变，则因子不变
                if contract_j != contract_i:
                    # 旧主力合约t-1天（前一天）的收盘价
                    close_old_t_1 = df.loc[df.index == i].close.values[0]
                    settlement_old_t_1 = df.loc[df.index == i].SETTLEMENT.values[0]

                    # 表示新主力合约t天（当天）的上一个交易日收盘价
                    if not df.loc[df[feature] == contract_j].empty:
                        pre_close_new_t = df.loc[df[feature] == contract_j].pre_close.values[0]
                        pre_settlement_new_t = df.loc[df[feature] == contract_j].pre_settlement.values[0]

                        # 更新因子
                        if not (pre_close_new_t is None and pre_settlement_new_t is None):
                            f_actorclose = f_actorclose * (close_old_t_1 / pre_close_new_t)
                            f_actorsettlement = f_actorsettlement * (settlement_old_t_1 / pre_settlement_new_t)
                # 更新合约
                contract_i = contract_j
            output_df = df

            # 新增字段dom_subdom
            if feature == 'dom_contract':
                output_df['dom_subdom'] = 'DOM'
            else:
                output_df['dom_subdom'] = 'SUBDOM'

            # 整理
            output_df = output_df[
                ['date', 'code', feature, 'close', 'SETTLEMENT', 'OPENINT', 'next_symbol', 'dom_subdom', 'pre_close',
                 'pre_settlement', 'factor_close', 'factor_settlement']]
            rename_dict = {feature: 'symbol', 'SETTLEMENT': 'settlement', 'OPENINT': 'open_interest'}

            output_df = output_df.rename(columns=rename_dict)
            return output_df

        dom_contract_info = get_factors(part_actorclose_dom, 'dom_contract')  # 主合约

        # 次主力合约
        part_dom = get_subdom(code)  # 获得主力合约
        # 从rawdata获得信息并且合并
        part_actorclose_subdom = part_dom.merge(
            rawdata[['DATE', 'CONTRACT', 'OPENINT', 'SETTLEMENT', 'symbol', 'close', ]], how='left',
            left_on=['date', 'subdom_contract'], right_on=['DATE', 'CONTRACT'])

        # 新增三个字段
        part_actorclose_subdom['pre_close'] = part_actorclose_subdom.apply(
            lambda x: get_pre_close(x, 'subdom_contract'), axis=1)
        part_actorclose_subdom['pre_settlement'] = part_actorclose_subdom.apply(
            lambda x: get_pre_settlement(x, 'subdom_contract'), axis=1)
        part_actorclose_subdom['next_symbol'] = part_actorclose_subdom.apply(
            lambda x: get_next_symbol(x, 'subdom_contract'), axis=1)

        subdom_contract_info = get_factors(part_actorclose_subdom, 'subdom_contract')

        all_contract_info = dom_contract_info.append(subdom_contract_info, ignore_index=True)

        return all_contract_info

    return contract_factors(code)


# 脚本运行

# 输出所有的期货品种每天的信息
# 首先输出第一种
type_info = get_info(code_list[0])
for i in range(1, len(code_list)):
    code_i = code_list[i]
    print(code_i)
    type_info = type_info.append(get_info(code_i), ignore_index = True)


# 输出并保存
type_info.to_csv('测试结果_余哲颖.csv', index = False )
