import pandas as pd

student_df = pd.DataFrame(
    {
        "Student_Id":[1,2,3,4,5],
        "Name":["James","John","Cindy","Bob","Mike"],
        "Gender":["Male","Male","Female","Male","Male"]
    }
)
print("=====before apply map function=====")
print(student_df.head())

map_values={"Male":"M","Female":"F"}

print("=====after apply map function=====")
student_df["gender_initials"] = student_df["Gender"].map(map_values)

print(student_df.head())
