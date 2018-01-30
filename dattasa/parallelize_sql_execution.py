import os
import pandas as pd
from dattasa import data_pipeline as dp


class ParallelSqlExecutor:

    def __init__(self, sql_file, log_file, out_file, sql_parm_list=[]):
        self.sql_file = sql_file
        self.log_file = log_file
        self.out_file = out_file
        self.sql_parm_list = sql_parm_list
        return

    def generate_employer_sets(self, emp_file=''):

        data_gp3 = dp.DataComponent().set_credentials('gp3')
        conn = data_gp3.get_db_conn(True , True , 20)

        query = """select employer_id
                               from strata_scratch.bb_account_attributes
                               WHERE employer_id NOT IN (52,53,89) group by 1;"""

        df = pd.read_sql(query, conn)
        full_list = df['employer_id'].values.tolist()

        if emp_file == '':
            emp_file = os.environ['STRATA_COMMON'] + '/employer_sets.cfg'

        with open(emp_file, 'r') as f:
            ''' Add each line in the file as 1 element of final_list '''
            content = f.readlines()
            final_list = [x.strip() for x in content]
            f.close()

        with open(emp_file , 'r') as f:
            ''' Determine all employer_ids added so far '''
            emp_added_list = []
            for row in f:
                temp_str = row.strip(',')
                emp_added_list += temp_str.split(',')
            f.close()

        ''' Convert Sttring to List '''
        emp_added_list = [int(x) for x in emp_added_list]
        ''' Find all employer ids that are missing '''
        missing_emp = list(set(full_list) - set(emp_added_list))
        ''' Convert to string and concatente before adding as last item of final_list '''
        missing_emp = [str(x) for x in missing_emp]

        ''' Add the missing emp_ids as last set in final_list '''
        final_list.append(','.join(missing_emp))

        index_list = 0
        parm_list = []
        for emp_ids in final_list:
            partition_id = index_list + 1
            if index_list == 0:
                ''' walmart - 2 partitions 1 for bcar and 1 for rest '''
                parm_list.append({"emp_id": str(emp_ids),
                                  "cond": "insurance_company_id = 15",
                                  "prt_name": 'emp_set_' + str(partition_id),
                                  "sub_prt_name": "bcar"})
                parm_list.append({"emp_id": str(emp_ids),
                                  "cond": "insurance_company_id != 15",
                                  "prt_name": 'emp_set_' + str(partition_id),
                                  "sub_prt_name": "other_carriers"
                                  })
            elif index_list == 1:
                print('skipping : ' + emp_ids)
            elif index_list in range(2, len(final_list) - 1):
                parm_list.append({"emp_id": str(emp_ids) ,
                                  "cond": "insurance_company_id != 15",
                                  "prt_name": 'emp_set_' + str(partition_id),
                                  "sub_prt_name": "other_carriers"})
            elif index_list == len(final_list) - 1:
                parm_list.append({"emp_id": str(emp_ids),
                                  "cond": "insurance_company_id != 15",
                                  "prt_name": "other",
                                  "sub_prt_name": "other_carriers"})
                parm_list.append({"emp_id": str(emp_ids) ,
                                  "cond": "insurance_company_id = 15",
                                  "prt_name": "other",
                                  "sub_prt_name": "bcar"})
            index_list += 1

        self.sql_parm_list = parm_list
        return

    def run_parallel_queries(self):

        if len(self.sql_parm_list) > 1:
            process_gp3 = dp.DataProcessor(source_conn='gp3')
            process_gp3.multiprocess_sql_query_run(self.sql_file, self.log_file,
                                                   self.out_file, self.sql_parm_list,
                                                   delimited_file=True, file_delimiter=',')
        else:
            data_gp3 = dp.DataComponent().set_credentials('gp3')
            data_gp3.run_psql_file(self.sql_file, self.log_file, self.out_file,
                                   delimited_file=True, file_delimiter=',')

        return


if __name__ == '__main__':
    sql_file = os.environ['STRATA_PROJECTS'] + '/building-blocks/src/sql-queries/generate_member_months.sql'
    log_file = os.environ['STRATA_LOG'] + '/generate_member_months.log'
    out_file = os.environ['STRATA_EXTRACTS'] + '/generate_member_months.out'
    process = ParallelSqlExecutor(sql_file, log_file, out_file)
    process.generate_employer_sets()
    process.run_parallel_queries()
