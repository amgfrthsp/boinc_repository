import pandas as pd

file = "hosts_power.raw"

df = pd.read_csv(file, delimiter='\t')
df.drop(columns=['CPU model', 'GFLOPs/computer'], inplace=True)
df = df.loc[df.index.repeat(df['Number of computers'])]
df.reset_index(drop=True, inplace=True)
df.drop(columns=['Number of computers'], inplace=True)
df['Avg. cores/computer'] = df['Avg. cores/computer'].round().astype(int)
df.rename(columns={'Avg. cores/computer': 'cores',
          'GFLOPS/core': 'cpu_speed'}, inplace=True)
df.to_csv('cpu_list.csv', index=False)
