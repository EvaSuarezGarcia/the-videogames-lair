# Get Hadoop
echo "Getting Hadoop 2.7.7"
wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz

echo "Extracting Hadoop..."
tar -xzvf hadoop-2.7.7.tar.gz

echo "Setting permissions..."
chown -R spark-user:spark-user hadoop-2.7.7

# Get Spark
echo "Getting Spark 2.4.4..."
wget http://apache.rediris.es/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz

echo "Extracting Spark..."
tar -xzvf spark-2.4.4-bin-hadoop2.7.tgz

echo "Setting permissions..."
chown -R spark-user:spark-user spark-2.4.4-bin-hadoop2.7

# Get MKL wrapper
echo "Getting MKL wrapper..."
git clone https://github.com/Intel-bigdata/mkl_wrapper_for_non_CDH.git

# Additional Spark configuration
cd spark-2.4.4-bin-hadoop2.7/conf

echo "Creating spark-env.sh"
cp spark-env.sh.template spark-env.sh
{
  echo "export PYSPARK_PYTHON=python3";
  echo "export PYSPARK_DRIVER_PYTHON=python3";
  echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:\$HADOOP_HOME/lib/native";
} >> spark-env.sh

echo "Creating spark-defaults.conf"
cp spark-defaults.conf.template spark-defaults.conf

{
  echo "# MKL settings";
  echo "spark.driver.extraJavaOptions=-Dcom.github.fommil.netlib.BLAS=com.intel.mkl.MKLBLAS -Dcom.github.fommil.netlib.LAPACK=com.intel.mkl.MKLLAPACK";
  echo "spark.driver.extraClassPath=/opt/intel/mkl/wrapper/mkl_wrapper.jar";
  echo "spark.executor.extraJavaOptions=-Dcom.github.fommil.netlib.BLAS=com.intel.mkl.MKLBLAS -Dcom.github.fommil.netlib.LAPACK=com.intel.mkl.MKLLAPACK";
  echo "spark.executor.extraClassPath=/opt/intel/mkl/wrapper/mkl_wrapper.jar";
} >> spark-defaults.conf

cd ../..

# Additional Hadoop configuration
cd hadoop-2.7.7/etc/hadoop
mv hadoop-env.sh hadoop-env.template
sed "s/export JAVA_HOME=.*/export JAVA_HOME=\/usr\/lib\/jvm\/java-8-oracle/g" hadoop-env.template > hadoop-env.sh

cd ../../..

echo "Time to move Spark and Hadoop, create env variables and install MKL!"

# Move Spark and Hadoop to /opt
echo "Moving Spark to /opt"
mv spark-2.4.4-bin-hadoop2.7 /opt/

echo "Adding environment variables to /etc/profile.d/spark.sh..."
{
  echo "export SPARK_HOME=/opt/spark-2.4.4-bin-hadoop2.7";
  echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin";
} >> /etc/profile.d/spark.sh

echo "Moving Hadoop to /opt..."
mv hadoop-2.7.7 /opt/

echo "Adding HADOOP_HOME to /etc/profile.d/hadoop.sh"
{
  echo "export HADOOP_HOME=/opt/hadoop-2.7.7";
  echo "export PATH=\$PATH:\$HADOOP_HOME/bin";
} >> /etc/profile.d/hadoop.sh

echo "Installing MKL..."
wget https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB
apt-key add GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB
sh -c 'echo deb https://apt.repos.intel.com/mkl all main > /etc/apt/sources.list.d/intel-mkl.list'
apt-get update
apt-get -y install intel-mkl-64bit-2019.5-075

echo "Configuring MKL"
update-alternatives --install /usr/lib/x86_64-linux-gnu/libblas.so \
                    libblas.so-x86_64-linux-gnu /opt/intel/mkl/lib/intel64/libmkl_rt.so 50
update-alternatives --install /usr/lib/x86_64-linux-gnu/libblas.so.3 \
                    libblas.so.3-x86_64-linux-gnu /opt/intel/mkl/lib/intel64/libmkl_rt.so 50
update-alternatives --install /usr/lib/x86_64-linux-gnu/liblapack.so \
                    liblapack.so-x86_64-linux-gnu /opt/intel/mkl/lib/intel64/libmkl_rt.so 50
update-alternatives --install /usr/lib/x86_64-linux-gnu/liblapack.so.3 \
                    liblapack.so.3-x86_64-linux-gnu  /opt/intel/mkl/lib/intel64/libmkl_rt.so 50

{
  echo "/opt/intel/lib/intel64";
  echo "/opt/intel/mkl/lib/intel64";
} >> /etc/ld.so.conf.d/mkl.conf
ldconfig

mkdir /opt/intel/mkl/wrapper
cd mkl_wrapper_for_non_CDH
cp mkl_wrapper.jar /opt/intel/mkl/wrapper/
cp mkl_wrapper.so /opt/intel/mkl/wrapper/

# Clean up
cd ..
rm GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB
rm -r mkl_wrapper_for_non_CDH
rm hadoop-2.7.7.tar.gz
rm spark-2.4.4-bin-hadoop2.7.tgz

echo "Finished!"
echo "To check that everything is ok run:"
echo "source /etc/profile.d/hadoop.sh"
echo "source /etc/profile.d/spark.sh"
echo "spark-shell"
echo "scala> import com.github.fommil.netlib.LAPACK;"
echo "scala> System.out.println(LAPACK.getInstance().getClass().getName());"
echo "scala> import com.github.fommil.netlib.BLAS;"
echo "scala> System.out.println(BLAS.getInstance().getClass().getName());"



