from prefect.filesystems import GitHub

def create_block():
    block = GitHub(
        repository="https://github.com/elijellyeli/data-zoomcamp.git"
    )
    # block.get_directory("flows") # specify a subfolder of repo
    block.save("zoom-github", overwrite=True)

if __name__ == "__main__":
    create_block()