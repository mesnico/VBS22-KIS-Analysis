from pathlib import Path
import pandas as pd


class Result:
    def __init__(self) -> None:
        pass

    def generate(self, cache=True, cache_path='cache', cache_filename=None, **kwargs):
        path = Path(cache_path)
        if cache:
            path.mkdir(parents=True, exist_ok=True)
        if cache_filename is None:
            fname = path / '{}.pkl'.format(type(self).__name__)
        else:
            fname = path / cache_filename

        # include some caching logic (cache df to pickle)
        if fname.exists() and cache:
            self.df = pd.read_pickle(fname)
        else:
            self.df = self._generate(**kwargs)
            if cache:
                self.df.to_pickle(fname)

    def render(self):
        self._render(self.df)

    def generate_and_render(self, kwargs):
        self.generate(**kwargs)
        self.render()
    