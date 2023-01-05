# Content-Based Filter Queries on DNA Data Storage Systems

This is a software suite that allows encoding relational database tables to DNA. The encoding scheme enables accessing the data based on its content using content-based barcodes (CBBs).

## Installation

Make sure you have [Java 17+](https://www.oracle.com/de/java/technologies/javase-downloads.html) and [Maven](https://maven.apache.org/download.cgi) installed. You need to clone the following project first:
```sh
git clone https://github.com/alexelshaikh/OpenRQ
```
Then, run the following command inside the project's root directory:
```sh
mvn install -DskipTests
```
or simply import the project with your IDE and install (and skip tests) using Maven it in your IDE. Note that we deployed the implementation from [OpenRQ](https://github.com/openrq-team/OpenRQ) into our own repository.

After that, you can finally build this project by running the following command in the root directory of this project.
```sh
mvn package
```
Alternatively, you can import this project into your IDE and run maven `package` from there.
The output executable _.jar_ file will be located at `/target/dna-system-1.0-jar-with-dependencies.jar`.


## Parameters
All parameters are set in the JSON config file `params.ini` that must be located in the same directory as the executable _.jar_ file. You can use your custom parameters' file by passing its path as the first argument to the program. There is an example `params.ini` in this project that can be used to encode/decode the dummy data set provided in `table.csv` in this project's root directory. This dummy data set is a small slice of the file [A321_valid.csv.xz ](https://opensky-network.org/datasets/publication-data/climbing-aircraft-dataset/trajs/) from the OpenSky Network. The `params.ini` file is well documented and easy to adapt to a different workload. Furthermore, each of the parameters is well documented in the `params.ini` file. 

## Usage

The program requires setting the correct parameters to start generating. Parameters can be set in the `params.ini` file in the same directory of the jar file. The parameters are parsed into the program by a JSON parser. See the following examples for usage.
### Example

First, run `package` to create the executable _.jar_ file. Next, copy `table.csv` and `params.ini` to the `target` directory. Then, run the following command from the command line (or shell) from the `target` directory:
```sh
java -jar dna-system-1.0-jar-with-dependencies.jar
```
The command above encodes the relational database table `table.csv` given `params.ini` to FASTA files located in the directory `target/encoded`. After that, it will decode the encoded FASTA files to `target/decoded`, as specified in `params.ini`. The following command uses a custom parameters' file in the same directory but named `custom_params.ini`:
```sh
java -jar dna-system-1.0-jar-with-dependencies.jar custom_params.ini
```


Furthermore, try modifying the parameters in `params.ini`, e.g., try to increase or decrease the number of `segmentation_permutations`, which set the number of permutations that are used when applying segmentation.

If you are encoding/decoding large amounts of data, consider [increasing the available heap space of the JVM](https://docs.oracle.com/cd/E29587_01/PlatformServices.60x/ps_rel_discovery/src/crd_advanced_jvm_heap.html). The following command is equivalent to the one above but allows the JVM to use up to 1000 GB of heap space.
```sh
java -jar -Xmx1000g dna-system-1.0-jar-with-dependencies.jar
```

## External Libraries
We are using the following JSON library: [org.json](https://github.com/stleary/JSON-java). This dependency is downloaded automatically by building the project.

We copied the [OpenRQ](https://github.com/openrq-team/OpenRQ) project into our own Maven project [here](https://github.com/alexelshaikh/OpenRQ) to make the import easier. This project should be installed before building our project.