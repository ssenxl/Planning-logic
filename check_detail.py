import pandas as pd

detail = pd.read_excel('c:/vscode/AI_plan/data_plan/booking_final_ready25.xlsx', sheet_name='DETAIL')
fd3 = detail[(detail['ITEM_CODE'] == 'FD3GNTPE54/14A0')]
print('DETAIL rows for FD3GNTPE54/14A0:')
for _, row in fd3.iterrows():
    w = row.get('WEEK', 'N/A')
    mc = row.get('MC_GROUP', 'N/A')
    mu = row.get('MC_USE', 'N/A')
    mcceil = row.get('MC_USE_CEIL', 'N/A')
    print(f'W{w} MC={mc}: MC_USE={mu}, MC_USE_CEIL={mcceil}')

print('\n--- SUMMARY week 19 IBP ---')
summary = pd.read_excel('c:/vscode/AI_plan/data_plan/booking_final_ready25.xlsx', sheet_name='SUMMARY_MC_REMAIN')
s19 = summary[(summary['WEEK'] == 19) & (summary['MC_GROUP'] == 'IBP')]
for _, row in s19.iterrows():
    print(f"W{row['WEEK']} {row['MC_GROUP']} G={row['GUAGE']}: TOTAL={row['TOTAL_MC']}, USE={row['MC_USE_CEIL']}, REMAIN={row['TOTAL_MC_REMAIN']}")
