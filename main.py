from utils.utils import cargar_repos, procesa_repo, hacer_repo, procesa_repos

if __name__ == '__main__':

    config_repos = cargar_repos()

    procesa_repos(config_repos)
