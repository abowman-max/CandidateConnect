from pathlib import Path
import pandas as pd

SOURCE = Path('candidate_connect_data.parquet')
INDEX_OUT = Path('candidate_connect_index.parquet')
DETAIL_OUT = Path('candidate_connect_detail.parquet')

# Keep all source data. We only split by purpose.
INDEX_PRIORITY = [
    'HH_ID','County','Municipality','Precinct','USC','STS','STH','School District',
    'Party','CalculatedParty','HH-Party','Gender','Sex','Age','Age_Range','Age Range','AGERANGE',
    'RegistrationDate','registrationdate','VoterStatus','voterstatus','V4A','MMB_AProp_Score','MIB_Applied','MIB_BALLOT',
    'MB_PERM','MB_Perm','MB_Pern','Email','Landline','Mobile','TAG0003_New_Reg',
    'TAG0001_Pro2A','TAG00011_Pro2A_FOAC_TARG','TAG0002_MB_Target','TAG0004_ProLife','TAG0005_ProLabor',
    'TAG0006_RepDonor','TAG0007_DemDonor','TAG0008_TrumpDonor','TAG00090_PADonor','TAG00100_FedDonor',
    'TAG00110_AllDonor','TAG0014_Teacher','TAG0015_RetiredTeacher',
    'House Number','Street Name','Apartment Number','FirstName','MiddleName','LastName','NameSuffix','FullName'
]

ID_CANDIDATES = ['PA ID Number','State Voter ID','Voter ID','VoterID','PA Voter ID']


def main():
    if not SOURCE.exists():
        raise FileNotFoundError(f'Missing source file: {SOURCE.resolve()}')

    df = pd.read_parquet(SOURCE)
    cols = list(df.columns)

    vm_cols = [c for c in cols if str(c).endswith('_VM')]
    id_cols = [c for c in ID_CANDIDATES if c in cols]
    index_cols = [c for c in INDEX_PRIORITY if c in cols] + vm_cols + id_cols
    # Preserve order and uniqueness
    seen = set()
    index_cols = [c for c in index_cols if not (c in seen or seen.add(c))]

    if not index_cols:
        raise ValueError('Could not identify index columns from source file.')

    index_df = df[index_cols].copy()
    detail_df = df.copy()

    index_df.to_parquet(INDEX_OUT, index=False)
    detail_df.to_parquet(DETAIL_OUT, index=False)

    print('Done')
    print(f'Index rows: {len(index_df):,} | cols: {len(index_df.columns):,}')
    print(f'Detail rows: {len(detail_df):,} | cols: {len(detail_df.columns):,}')
    print(f'Created: {INDEX_OUT.name}')
    print(f'Created: {DETAIL_OUT.name}')


if __name__ == '__main__':
    main()
