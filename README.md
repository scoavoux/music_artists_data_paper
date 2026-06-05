

## replication

Due to data sharing and privacy issues, the pipeline cannot be replicated with
the original data. Hence, we provide a small dataset that allows to simulate the
data production. Only the named entity recognition (NER) that is applied to the press 
corpus runs outside this script. It can be found in `./python_press/ner_on_press_new.ipynb`.
The simulation data is located in the `data/` folder.

To install all R dependencies, run

```
Rscript -e "source("renv/activate.R); renv::restore()"
```
from the root project folder.

To replicate the pipeline with the provided simulation data in the `data/` folder, run

```
Rscript -e "targets::tar_make()"
```
from the root project folder. Approximate runtime is 10 seconds.

