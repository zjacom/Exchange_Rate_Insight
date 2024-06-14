import glob
import importlib.util
import os

import pytest
from airflow.models import DAG

from airflow.utils.dag_cycle_tester import test_cycle as dag_test_cycle

DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "dags/**/*.py")
# DAG_PATH로 찾은 모든 파이썬 파일을 리스트로 보유
DAG_FILES = glob.glob(DAG_PATH, recursive=True)

@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file):
    # 파이썬 파일들을 로드하고 실행한 다음 DAG 객체를 추출하는 과정
    module_name, _ = os.path.splitext(dag_file)
    module_path = os.path.join(DAG_PATH, dag_file)
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)
    
    dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]
    
    # DAG 객체를 성공적으로 찾았는지 테스트한다.
    # 즉, dags 폴더에 DAG 객체만 존재하는지 확인할 수 있다.
    assert dag_objects
    
    # DAG 객체에 순환주기가 있는지 테스트한다.
    for dag in dag_objects:
        dag_test_cycle(dag)

if __name__ == "__main__":
    pytest.main()