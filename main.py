# x= 1
# try:
#     result=5/x
#     print(f"The answer is {result}")
# except ZeroDivisionError:
#     print("Error: Division by zero")

# else:
#     print("I am the else clause")
# finally:
#     print("I am the finally clause")


# def divide(a,b):
#     if b==0:
#         raise ValueError("Cannot divide by zero")
#     return a/b
# try:
#     result = divide(10, 0)
# except ZeroDivisionError as e:
#     print(f"This is ZeroDivisionError handling:{e}")


# class Person:
#     def __init__(self, name='Levis',skills=None) -> None:
#         self.name:str=name
#         if skills is None:
#             self.skills=[]
#         else:
#             self.skills = skills


#     def personal_info(self)->str:
#         return print(f'My name is {self.name}')
#     def add_skills(self,skill)->str:
#         self.skills.append(skill)

# p1 = Person('Levis')
# p1.add_skills('Python')
# print(p1.skills)

# p2 = Person('Alice')
# print(p2.skills)


class Statistics():
    def __init__(self, data) -> None:
        self.data=data
        self.data.sort()

    def count(self):
        """Returns the numnber of data points"""
        return len(self.data)

    def min(self):
        """Returns the minimum value from data set"""
        return self.data[0]
    def max(self):
        """Returns max number"""
        return self.data[-1]
    def sum(self):
        """Returns the sum of all the data points"""
        return sum(self.data)
    
    def range(self):
        """Returns the range of span for the data set"""
        return self.max()- self.min()
    
    def mean(self):
        """Returns the mean of the data set"""
        return round(self.sum()/self.count,2)
    
    def median(self):
        """Returns median of sorted data set"""
        n=self.count()
        mid =n//2
        print(f"Middle position is:{mid}")
        """Use a modulo operator to find is the data point count is even or odd"""
        if n % 2 == 0:
            """The 2 middle are at position mid-1 and mid, due to zero based indexing"""
            median = (self.data[mid - 1] + self.data[mid]) / 2
            return print(median)
        else:
            """If odd number of points, return middle value"""
            return print(self.data[mid])


heights = [160, 165, 168, 170, 172, 175, 178, 180, 182, 185,123,345]

s=Statistics(heights)
s.median()