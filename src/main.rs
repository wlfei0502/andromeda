use liter_llm::{ChatCompletionRequest, ClientConfigBuilder, DefaultClient, LlmClient, Message, UserMessage};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let api_key = "F6vRUzuaP0aND47QMZGS08rbK8y_qYckqkOEEvXrKT24qPto_gDRwyh_L6ZTtOCL67Oa8BvkjZ-GkQzBCoUXQg";
    let base_url = "http://maas.tianditu.gov.cn/v2/infer/21adbf42-3d12-4031-a74f-67372ec8ce34/v1";
    let model_name = "deepseek-v4-flash-0731";
    let config = ClientConfigBuilder::new(api_key).base_url(base_url).build();
    let client = DefaultClient::new(config, Some(model_name))?;
    let request = ChatCompletionRequest {
        model: model_name.into(),
        messages: vec![Message::User(UserMessage {
            content: "Hello, world!".into(),
            name: None,
        })],
        ..Default::default()
    };

    let response = client.chat(request).await?;

    if let Some(choice) = response.choices.first() {
        let text = choice.message.content.as_ref().and_then(|content| content.as_text() );
        println!("{}", text.unwrap_or_default());
    }

    Ok(())
}
