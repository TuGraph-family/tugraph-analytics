import os
import torch
torch.set_num_threads(1)

# class TorchInferSession(object):
#     def __init__(self, transform_class) -> None:
#         self._transform = transform_class
#         self._model_path = os.getcwd() + "/model.pt"
#         self._model = transform_class.load_model(self._model_path)
#
#     def run(self, *inputs):
#         feature = self._transform.transform_pre(*inputs)
#         res = self._model(*feature)
#         return self._transform.transform_post(res)

class TorchInferSession(object):
    def __init__(self, transform_class) -> None:
        self._transform = transform_class

    def run(self, *inputs):
        a,b = self._transform.transform_pre(*inputs)
        return self._transform.transform_post(a)

