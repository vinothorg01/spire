## Intro to Spire Submodules  

Spire uses github submodules to integrate different services e.g. Kalos, Datasci-Commons, and Spire-Astronomer into Spire. They exist in the /include/ directory, and the basic configuration can be seen in `.gitmodules`.  
  
As per the `README.md`, these submodules can be initiated via `git submodule update --init --recursive`, which will update the submodules to their appropriate respective commits as defined in the local commit of Spire.  
  
### How to update  
  
If changes are made to one of the submodules, it may be necessary to update the commit sha of the submodule within Spire. To do so, simply cd into the appropriate submodule, checkout the desired branch and pull the desired commit, and then `git add` the updated submodule into a commit for Spire.  
  
In such a case, it may also be necessary to reinstall the submodule and/or its requirements.  
  
It is suggested that if you reinstall a submodule, you also reinstall Spire itself, as there may be conflicting package versions and we currently have limited package management, and it is best to default to the requirements of Spire itself (or update them to match a submodule's if necessary).  

### CLI as a Pseudo-Submodule
  
While the CLI is not its own repository, it can be installed as its own package, making it somewhat like a submodule unto itself. It does not require git management, but the CLI may occassionally need to be reinstalled between branches or when certain updates are made.  
