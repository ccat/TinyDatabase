



class Set(object):

    def __init__(self,elements):
        if(isinstance(elements,list)):
            self.elements = elements
        else:
            raise Exception()

    def is_element(self,element):
        return element in self.elements

