-- Connect to MariaDB as root
mysql -u root -p

-- Create database
CREATE DATABASE excel_import CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create dedicated user (recommended for production)
CREATE USER 'excel_user'@'localhost' IDENTIFIED BY 'secure_password_123';
CREATE USER 'excel_user'@'%' IDENTIFIED BY 'secure_password_123'; -- For remote access

-- Grant privileges
GRANT ALL PRIVILEGES ON excel_import.* TO 'excel_user'@'localhost';
GRANT ALL PRIVILEGES ON excel_import.* TO 'excel_user'@'%';

-- Refresh privileges
FLUSH PRIVILEGES;

-- Verify user creation
SELECT User, Host FROM mysql.user WHERE User = 'excel_user';

-- Exit MySQL
EXIT;