import * as TelegramBot from 'node-telegram-bot-api';

export class TelegramNotification {
  private bot: TelegramBot;
  private chatId: string;

  constructor(apiKey: string, chatId: string) {
    this.bot = new TelegramBot(apiKey, { polling: true });
    this.chatId = chatId;

    this.bot.onText(/\/clear/, async (msg) => {
      try {
        for (let i = 0; true; i++) {
          await this.bot.deleteMessage(msg.chat.id, msg.message_id - i);
        }
      } catch (e) {}
    });
  }

  sendNotification = (message: string) => {
    this.bot.sendMessage(this.chatId, message);
  };
}
