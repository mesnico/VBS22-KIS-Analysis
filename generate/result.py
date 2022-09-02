from pathlib import Path
import pandas as pd


class Result:
    def __init__(self, use_cache=True, cache_path='cache/results', cache_filename=None) -> None:
        self.use_cache = use_cache
        self.cache_path = cache_path
        self.cache_filename = cache_filename

    def generate(self, **kwargs):
        path = Path(self.cache_path)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
        if self.cache_filename is None:
            fname = path / '{}.pkl'.format(type(self).__name__)
        else:
            fname = path / self.cache_filename

        # include some caching logic (cache df to pickle)
        if fname.exists() and self.use_cache:
            self.df = pd.read_pickle(fname)
        else:
            self.df = self._generate(**kwargs)
            self.df.to_pickle(fname)

    def render(self, **kwargs):
        self._render(self.df, **kwargs)

    def generate_and_render(self, generate_kwargs, render_kwargs):
        self.generate(**generate_kwargs)
        self.render(**render_kwargs)
    