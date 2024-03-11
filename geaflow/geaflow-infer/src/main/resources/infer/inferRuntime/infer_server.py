import argparse
import importlib
import os
import signal
import sys
import threading
import time
import traceback
from inferSession import TorchInferSession
from pickle_bridge import PicklerDataBridger

class check_ppid(threading.Thread):
    def __init__(self, name, daemon):
        super().__init__(name=name, daemon=daemon)

    def run(self) -> None:
        while os.getppid() != 1:
            time.sleep(1)
        os.kill(os.getpid(), signal.SIGKILL)

def parse_dir_script(script_path):
    index = str(script_path).rindex('/')
    dir_str = script_path[0: index + 1]
    script_name = script_path[index + 1: len(script_path) - 3]
    return dir_str, script_name


def get_user_define_class(class_name):
    transform_path = os.getcwd() + "/TransFormFunctionUDF.py"
    dir_name = parse_dir_script(transform_path)
    sys.path.insert(0, dir_name[0])
    user_py = importlib.import_module(dir_name[1])
    if hasattr(user_py, class_name):
        transform_class = getattr(user_py, class_name)
        return transform_class()
    else:
        raise ValueError("class name = {} not found".format(class_name))


def start_infer_process(class_name, output_queue_shm_id, input_queue_shm_id):
    transform_class = get_user_define_class(class_name)
    infer_session = TorchInferSession(transform_class)
    input_size = transform_class.input_size
    data_exchange = PicklerDataBridger(input_queue_shm_id, output_queue_shm_id, input_size)
    check_thread = check_ppid('check_process', True)
    check_thread.start()
    count = 0
    while True:
        try:
            inputs = data_exchange.read_data()
            if not inputs:
                count += 1
                if count % 1000 == 0:
                    time.sleep(0.05)
                    count = 0
            else:
                res = infer_session.run(*inputs)
                data_exchange.write_data(res)
        except Exception as e:
            exc_type, exc_val, exc_tb = sys.exc_info()
            error_msg = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            data_exchange.write_data('python_exception: ' + error_msg)
            sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tfClassName", type=str,
                        help="user define transformer class name")
    parser.add_argument("--input_queue_shm_id", type=str, help="input queue "
                                                               "share memory "
                                                               "id")
    parser.add_argument("--output_queue_shm_id", type=str,
                        help="output queue share memory id")
    args = parser.parse_args()
    start_infer_process(args.tfClassName, args.output_queue_shm_id,
                        args.input_queue_shm_id)
