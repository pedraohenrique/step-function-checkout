'use strict';
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const {
  DynamoDBDocumentClient,
  UpdateCommand,
  QueryCommand,
} = require('@aws-sdk/lib-dynamodb');
const {
  SFNClient,
  SendTaskSuccessCommand,
  SendTaskFailureCommand,
} = require('@aws-sdk/client-sfn');

const client = new DynamoDBClient({ region: process.env.AWS_REGION });
const ddbDocClient = DynamoDBDocumentClient.from(client);
const BOOK_TABLE_NAME = 'bookTable';
const USER_TABLE_NAME = 'userTable';

const StepFunction = new SFNClient({ region: process.env.AWS_REGION });

const isBookAvailable = (book, quantity) => {
  return book.quantity - quantity > 0;
};

module.exports.checkInventory = async ({ bookId, quantity }) => {
  try {
    const params = {
      TableName: BOOK_TABLE_NAME,
      KeyConditionExpression: 'bookId = :bookId',
      ExpressionAttributeValues: {
        ':bookId': bookId,
      },
    };

    const result = await ddbDocClient.send(new QueryCommand(params));
    const book = result.Items[0];

    if (isBookAvailable(book, quantity)) {
      return book;
    } else {
      const bookOutOfStockError = new Error('The book is out of stock');
      bookOutOfStockError.name = 'BookOutOfStock';
      throw bookOutOfStockError;
    }
  } catch (err) {
    if (err.name === 'BookOutOfStock') {
      throw err;
    } else {
      const bookNotFoundError = new Error(err);
      bookNotFoundError.name = 'BookNotFound';
      throw bookNotFoundError;
    }
  }
};

module.exports.calculateTotal = async ({ book, quantity }) => {
  const total = book.price * quantity;
  return { total };
};

const deductPoints = async (userId) => {
  const params = {
    TableName: USER_TABLE_NAME,
    Key: { userId },
    UpdateExpression: 'set points = :zero',
    ExpressionAttributeValues: {
      ':zero': 0,
    },
    ConditionExpression: 'attribute_exists(userId)',
  };
  await ddbDocClient.send(new UpdateCommand(params));
};

module.exports.redeemPoints = async ({ userId, total }) => {
  let orderTotal = total.total;
  try {
    const params = {
      TableName: USER_TABLE_NAME,
      KeyConditionExpression: 'userId = :userId',
      ExpressionAttributeValues: {
        ':userId': userId,
      },
    };

    const result = await ddbDocClient.send(new QueryCommand(params));
    const user = result.Items[0];

    const points = user.points;
    if (orderTotal > points) {
      await deductPoints(userId);
      orderTotal = orderTotal - points;
      return { total: orderTotal, points };
    } else {
      throw new Error('Order total is less than redeem points');
    }
  } catch (err) {
    throw new Error(err);
  }
};

module.exports.billCustomer = async (params) => {
  /* Bill the customer e.g. using stripe token from the parameters */
  return 'Successfully billed';
};

module.exports.restoreRedeemPoints = async ({ userId, total }) => {};

const updateBookQuantity = async (bookId, orderQuantity) => {
  console.log('bookId: ', bookId);
  console.log('orderQuantity: ', orderQuantity);
  let params = {
    TableName: BOOK_TABLE_NAME,
    Key: { bookId: bookId },
    UpdateExpression: 'SET quantity = quantity - :orderQuantity',
    ExpressionAttributeValues: {
      ':orderQuantity': orderQuantity,
    },
  };
  await ddbDocClient.send(new UpdateCommand(params));
};

module.exports.sqsWorker = async (event) => {
  console.log(JSON.stringify(event));
  const record = event.Records[0];
  const body = JSON.parse(record.body);
  try {
    // Find a courier and attach courier information to the order
    const courier = 'pedraohenrique@gmail.com';

    // update book quantity
    await updateBookQuantity(body.Input.bookId, body.Input.quantity);

    // throw "Something wrong with Courier API:

    // Attach courier information to the order
    const params = {
      output: JSON.stringify({ courier }),
      taskToken: body.Token,
    };
    await StepFunction.send(new SendTaskSuccessCommand(params));
  } catch (err) {
    console.log('===== You got an Error =====');
    console.log(err);
    const params = {
      error: 'NoCourierAvailable',
      cause: 'No couriers are available',
      taskToken: body.Token,
    };
    await StepFunction.send(new SendTaskFailureCommand(params));
  }
};

module.exports.restoreQuantity = async ({ bookId, quantity }) => {
  let params = {
    TableName: BOOK_TABLE_NAME,
    Key: { bookId: bookId },
    UpdateExpression: 'set quantity = quantity + :orderQuantity',
    ExpressionAttributeValues: {
      ':orderQuantity': quantity,
    },
  };
  await ddbDocClient.send(new UpdateCommand(params));
  return 'Quantity restored';
};
