from oppnadata_org_sync import OppnaDataOrgSync

if __name__ == '__main__':
    with OppnaDataOrgSync() as o:
        o.sync()