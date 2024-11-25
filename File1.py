# import sys 
# # print('Hello, World!')
# class PipelineDispatcher:
#     def __init__(self):
#         pass
#     def get_value(self, value):
#         return value
#     def run(self, value):
#         Value= self.get_value(value)
#         print('Done')
#         result= Value+1
#         print(result)
#         return(result)

# if __name__ == '__main__':
#     value = float(sys.argv[1])
#     dispatcher = PipelineDispatcher()
#     dispatcher.run(value)
# script1.py
import sys

class PipelineDispatcher:
    def __init__(self):
        pass
    
    def get_value(self, value):
        return value
    
    def run(self, value):
        value = self.get_value(value)
        return str(value + 1)

if __name__ == '__main__':
    value = float(sys.argv[1])
    dispatcher = PipelineDispatcher()
    result= dispatcher.run(value)
    print(result)
