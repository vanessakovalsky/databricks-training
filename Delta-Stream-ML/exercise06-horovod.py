# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 06 : Horovod Runner on Databricks Runtime for ML
# MAGIC
# MAGIC In Spark, you can train not only statistical model (such like, linear regressor, decision tree classifier, etc), but also you can train neural networks with TensorFlow, PyTorch, and so on. Here I show you TensorFlow training example with distributed manners, using ML runtime on Databricks.    
# MAGIC As you know, many of deep learning frameworks itself is having capabilities for distributed training without Apache Spark. (For instance, TensorFlow is natively having "Distributed TensorFlow".) However, it'll still be useful to run distributed training in Apache Spark, since you can integrate other operations with the same consistent distributed manners, such as, data preparation, data transformation, other machine learning tasks, so on and so forth.
# MAGIC
# MAGIC Before starting,
# MAGIC
# MAGIC 1. Run Exercise 01 (Storage Settings) in order to mount ```/mnt/testblob```.
# MAGIC 2. Copy [pickle dataset for MNIST](https://github.com/tsmatz/azure-databricks-exercise) ("MNIST_train_rank0.pkl", "MNIST_train_rank1.pkl", and "MNIST_test.pkl") in this mounted blob container (```/mnt/testblob```).
# MAGIC 3. Make sure to create **Databricks Runtime ML** for cluster and attach in this notebook. (You cannot run this exercise in the standard Databricks runtime without "ML".)
# MAGIC
# MAGIC ML runtime is optimized for deep learning, and all related components (TensorFlow, Horovod, Keras, XGBoost, etc) are already built-in. (You don’t need to install these components by yourself.)    
# MAGIC Built-in ```HorovodRunner``` on ML runtime helps Horovod to run on Apache Spark. (Horovod (by Uber) has efficient parameter sharing mechanism and beneficial for scaling.)
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC Make directory to save the trained model.

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/testblob/horovod_trained_model")

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare for the training function, in which we train the model to predict hand-writing digits images.

# COMMAND ----------

def train_fn(checkpoint_path, learning_rate=0.01):
  from tensorflow.keras import backend as K
  import tensorflow as tf
  from tensorflow import keras
  from tensorflow.keras import models
  from tensorflow.keras import layers
  import horovod.tensorflow.keras as hvd
  import numpy as np
  import pandas as pd

  hvd.init()

  # use different data in each workers
  train_dat = pd.read_pickle("/dbfs/mnt/testblob/MNIST_train_rank%d.pkl" % hvd.rank())
  x_train = train_dat["image"]
  x_train = np.stack(x_train.values, axis=0)
  y_train = train_dat["label_col"]
  y_train = keras.utils.to_categorical(y_train, 10)

  # generate model
  model = models.Sequential([
    layers.Dense(784, activation='relu', input_shape=(784,)),
    layers.Dense(128, activation='relu'),
    layers.Dense(64, activation='relu'),
    layers.Dense(10, activation='softmax'),
  ])

  # prepare optimizer and compile to TensorFlow model with Keras
  optimizer = keras.optimizers.SGD(
    learning_rate=learning_rate * hvd.size(),
    momentum=0.9)
  optimizer = hvd.DistributedOptimizer(optimizer)
  model.compile(
    optimizer=optimizer,
    loss='categorical_crossentropy',
    metrics=['accuracy'])

  # save checkpoint only on rank 0 to prevent conflicts
  callbacks = [hvd.callbacks.BroadcastGlobalVariablesCallback(0)]
  if hvd.rank() == 0:
    callbacks.append(keras.callbacks.ModelCheckpoint(
      checkpoint_path,
      save_weights_only=True))

  # train
  model.fit(
    x_train,
    y_train,
    batch_size=64,
    callbacks=callbacks,
    epochs=2,
    verbose=2)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the training with ```HorovodRunner```. Above function will be run on distributed workers (executors).

# COMMAND ----------

from sparkdl import HorovodRunner

# run only 2 workers (rank0 and rank1)
hr = HorovodRunner(np=2)
hr.run(
  main=train_fn,
  checkpoint_path="/dbfs/mnt/testblob/horovod_trained_model/checkpoint.ckpt",
  learning_rate=0.01)

# COMMAND ----------

# MAGIC %md
# MAGIC Using above trained model, predict and compare the results with actual label.

# COMMAND ----------

# Check result (model) using test data
from tensorflow.keras import backend as K
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import models
from tensorflow.keras import layers
import numpy as np
import pandas as pd

# load test data
test_dat = pd.read_pickle("/dbfs/mnt/testblob/MNIST_test.pkl")
x_test = test_dat["image"]
x_test = np.stack(x_test.values, axis=0)
y_test_label = test_dat["label_col"]

# load trained model
model = models.Sequential([
  layers.Dense(784, activation='relu', input_shape=(784,)),
  layers.Dense(128, activation='relu'),
  layers.Dense(64, activation='relu'),
  layers.Dense(10, activation='softmax'),
])
model.compile(
  optimizer=keras.optimizers.SGD(learning_rate=0.01, momentum=0.9),
  loss='categorical_crossentropy',
  metrics=['accuracy'])
model.load_weights(
  "/dbfs/mnt/testblob/horovod_trained_model/checkpoint.ckpt")

# predict and list results
results = model.predict(x_test)
pred_array = np.column_stack((y_test_label.values, np.argmax(results, axis=1)))
pred_df = pd.DataFrame(
  data = pred_array,
  columns = ["Actual", "Predicted"])
pred_df

# COMMAND ----------

# MAGIC %md
# MAGIC Remove the trained model. (Clean-up)

# COMMAND ----------

dbutils.fs.rm("/mnt/testblob/horovod_trained_model",recurse=True)