import pandas as pd
import json
from pathlib import Path
from typing import Union, Optional

def export_to_csv(df: pd.DataFrame, output_file: Union[str, Path]) -> bool:
    """
    Xuất DataFrame sang file CSV
    
    Args:
        df (DataFrame): DataFrame cần xuất
        output_file (str/Path): Đường dẫn file đầu ra
        
    Returns:
        bool: True nếu xuất thành công, False nếu thất bại
    """
    try:
        # Chuyển đổi sang Path object
        output_path = Path(output_file)
        
        # Tạo thư mục nếu chưa tồn tại
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Xuất dữ liệu
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        return True
    except Exception as e:
        print(f"Lỗi khi xuất CSV: {e}")
        return False

def export_to_excel(df: pd.DataFrame, output_file: Union[str, Path], 
                   sheet_name: str = "Comments") -> bool:
    """
    Xuất DataFrame sang file Excel
    
    Args:
        df (DataFrame): DataFrame cần xuất
        output_file (str/Path): Đường dẫn file đầu ra
        sheet_name (str): Tên sheet
        
    Returns:
        bool: True nếu xuất thành công, False nếu thất bại
    """
    try:
        # Chuyển đổi sang Path object
        output_path = Path(output_file)
        
        # Tạo thư mục nếu chưa tồn tại
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Xuất dữ liệu
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Tạo định dạng cho workbook
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            
            # Định dạng tiêu đề
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'bg_color': '#D7E4BC',
                'border': 1
            })
            
            # Áp dụng định dạng cho tiêu đề
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                
            # Tự động điều chỉnh độ rộng cột
            for i, col in enumerate(df.columns):
                column_width = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, column_width)
        
        return True
    except Exception as e:
        print(f"Lỗi khi xuất Excel: {e}")
        return False

def export_to_json(df: pd.DataFrame, output_file: Union[str, Path], 
                  orient: str = 'records', indent: int = 4) -> bool:
    """
    Xuất DataFrame sang file JSON
    
    Args:
        df (DataFrame): DataFrame cần xuất
        output_file (str/Path): Đường dẫn file đầu ra
        orient (str): Cách định dạng JSON (records, split, index, columns, values)
        indent (int): Số khoảng trắng để thụt đầu dòng
        
    Returns:
        bool: True nếu xuất thành công, False nếu thất bại
    """
    try:
        # Chuyển đổi sang Path object
        output_path = Path(output_file)
        
        # Tạo thư mục nếu chưa tồn tại
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Chuyển đổi DataFrame sang dict
        json_data = df.to_dict(orient=orient)
        
        # Xuất dữ liệu
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=indent)
        
        return True
    except Exception as e:
        print(f"Lỗi khi xuất JSON: {e}")
        return False

def export_to_html(df: pd.DataFrame, output_file: Union[str, Path], 
                  title: str = "TikTok Comments Data") -> bool:
    """
    Xuất DataFrame sang file HTML
    
    Args:
        df (DataFrame): DataFrame cần xuất
        output_file (str/Path): Đường dẫn file đầu ra
        title (str): Tiêu đề trang HTML
        
    Returns:
        bool: True nếu xuất thành công, False nếu thất bại
    """
    try:
        # Chuyển đổi sang Path object
        output_path = Path(output_file)
        
        # Tạo thư mục nếu chưa tồn tại
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Tạo HTML
        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
        }}
        h1 {{
            color: #2c3e50;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin-top: 20px;
        }}
        th, td {{
            text-align: left;
            padding: 8px;
            border: 1px solid #ddd;
        }}
        th {{
            background-color: #f2f2f2;
            color: #333;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        tr:hover {{
            background-color: #f1f1f1;
        }}
        .info {{
            margin-bottom: 20px;
            color: #555;
        }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <div class="info">
        <p>Tổng số bình luận: {len(df)}</p>
        <p>Thời gian xuất: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    {df.to_html(index=False)}
</body>
</html>
"""
        
        # Lưu HTML
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return True
    except Exception as e:
        print(f"Lỗi khi xuất HTML: {e}")
        return False