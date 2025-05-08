-- 创建主密钥（仅首次需要执行）
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword!123';
GO

-- 创建证书，内部包含非对称加密所需的公钥和私钥
CREATE CERTIFICATE MyCertificate WITH SUBJECT = 'Encryption Certificate';
GO

-- 使用证书公钥对数据进行加密
DECLARE @plainText NVARCHAR(100) = '敏感数据';
DECLARE @encrypted VARBINARY(MAX) = ENCRYPTBYCERT(CERT_ID('MyCertificate'), CAST(@plainText AS NVARCHAR(MAX)));
SELECT @encrypted AS EncryptedData;
GO

-- 可选：使用证书私钥对加密数据进行解密
DECLARE @decrypted NVARCHAR(MAX);
SELECT @decrypted = CAST(DECRYPTBYCERT(CERT_ID('MyCertificate'), @encrypted) AS NVARCHAR(MAX));
SELECT @decrypted AS DecryptedData;
GO
